/**
 * @zakkster/lite-channel v1.0.0
 * ----------------------------------------------------------------------------
 * Cross-tab synchronization for @zakkster/lite-signal over BroadcastChannel.
 *
 * One channel, many signals (multiplexed by key). Conflict resolution is a
 * last-writer-wins register ordered by a Lamport clock with a stable per-tab
 * id as the tiebreaker, so concurrent writes converge deterministically instead
 * of diverging. Presence (peer count, leader election, connection status) is
 * exposed AS reactive signals, so an app can react to the cluster the same way
 * it reacts to data.
 *
 * == Why signals, not callbacks ==
 * `peers`, `status`, `isLeader`, and `members` are read-only signals. A consumer
 * writes `effect(() => badge.textContent = peers() + " tabs")` and gets live
 * presence for free; gate leader-only work with `effect(() => { if (isLeader()) … })`.
 *
 * == Outbound coalescing (ecosystem tie-in) ==
 * A burst of synchronous writes collapses to a single broadcast carrying the
 * final value. The flush is driven by an injectable `schedule` (default:
 * microtask). Pass a frame-cadence scheduler (e.g. adapted from @zakkster/lite-raf)
 * to broadcast at most once per animation frame, or `(f) => f()` for synchronous
 * flushing in tests.
 *
 * == Conflict model ==
 * Per key we keep `(clock, lastWriter)`. A remote update is accepted iff
 * `remote.clock > local.clock || (remote.clock === local.clock && remote.writer
 * > local.writer)`. On accept we take `max` of the clocks; on local write we
 * increment. This is a CRDT LWW-register: every tab applying the same set of
 * updates ends in the same state regardless of delivery order.
 *
 * @module @zakkster/lite-channel
 */

import {signal, computed, batch} from "@zakkster/lite-signal";

/** Wrap a signal/computed as a read-only accessor (callable + peek + subscribe). */
function readonly(sig) {
    const r = () => sig();
    r.peek = sig.peek;
    r.subscribe = sig.subscribe;
    return r;
}

/** Stable, collision-resistant per-tab id. */
function newTabId() {
    const c = globalThis.crypto;
    if (c && typeof c.randomUUID === "function") return c.randomUUID();
    return Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 10);
}

/**
 * Create a multiplexed tab-sync bus over a single BroadcastChannel.
 *
 * @param {string} channelName Origin-scoped channel name shared by all tabs.
 * @param {object} [options]
 * @param {(flush: () => void) => void} [options.schedule] Outbound flush scheduler.
 *        Default `(f) => queueMicrotask(f)`. Use `(f) => f()` for synchronous flush.
 * @param {boolean} [options.persist=true] Mirror each key to localStorage as a
 *        lone-tab cold-start fallback (values must be JSON-serialisable).
 * @param {number} [options.heartbeatMs=2000] Presence re-announce interval. `0` disables.
 * @param {number} [options.evictMs=5000] Drop a peer not heard from within this window.
 * @param {number} [options.readyMs=150] If no snapshot arrives within this window,
 *        assume we are the first tab and flip `status` to "synced".
 * @param {(err: unknown) => void} [options.onError] Error sink for clone/storage failures.
 * @returns {{
 *   channelName: string, tabId: string,
 *   sync: (sig: any, key?: string) => { dispose: () => void },
 *   peers: any, status: any, isLeader: any, members: any,
 *   dispose: () => void,
 * }}
 */
export function createTabSync(channelName, options = {}) {
    const schedule = options.schedule || ((f) => queueMicrotask(f));
    const persist = options.persist !== false;
    const heartbeatMs = options.heartbeatMs != null ? options.heartbeatMs : 2000;
    const evictMs = options.evictMs != null ? options.evictMs : 5000;
    const readyMs = options.readyMs != null ? options.readyMs : 150;
    const onError = options.onError || ((err) => console.error("lite-channel:", err));

    const BC = globalThis.BroadcastChannel;
    if (typeof BC !== "function") {
        throw new Error("lite-channel: BroadcastChannel is unavailable in this environment");
    }

    const tabId = newTabId();
    const channel = new BC(channelName);
    const hasStorage = persist && typeof localStorage !== "undefined";

    // ── Reactive membership / presence ──────────────────────────────────────
    const members = signal([tabId]);                  // sorted tab ids incl. self
    const peers = computed(() => members().length - 1);
    const isLeader = computed(() => members()[0] === tabId);
    const status = signal("connecting");              // "connecting" | "synced"
    const lastSeen = new Map();                        // peerId -> timestamp

    function rememberPeer(id) {
        if (id === tabId) return;
        const isNew = !lastSeen.has(id);
        lastSeen.set(id, Date.now());
        if (isNew) {
            const next = members().slice();
            next.push(id);
            next.sort();
            members.set(next);
            post({t: "join", id: tabId});            // re-announce so the newcomer learns us
        }
    }

    function forgetPeer(id) {
        if (!lastSeen.has(id)) return;
        lastSeen.delete(id);
        members.set(members().filter((m) => m !== id));
    }

    // ── Synced keys ─────────────────────────────────────────────────────────
    // key -> { key, sig, clock, lastWriter, primed, applying, stopSub }
    const keys = new Map();
    const dirty = new Set();
    let pendingFlush = false;

    function post(msg) {
        try {
            channel.postMessage(msg);
        } catch (err) {
            onError(err);
        }                  // e.g. non-cloneable value
    }

    function storageKey(key) {
        return "lite-channel:" + channelName + ":" + key;
    }

    function persistKey(key, value, clock, writer) {
        if (!hasStorage) return;
        try {
            localStorage.setItem(storageKey(key), JSON.stringify({value, clock, w: writer}));
        } catch (err) {
            onError(err);
        }
    }

    function scheduleFlush() {
        if (pendingFlush) return;
        pendingFlush = true;
        schedule(flush);
    }

    function flush() {
        pendingFlush = false;
        if (dirty.size === 0) return;
        const updates = [];
        for (const key of dirty) {
            const k = keys.get(key);
            if (!k) continue;
            k.clock += 1;                              // local event advances the clock
            k.lastWriter = tabId;
            k.hasState = true;
            const value = k.sig.peek();
            updates.push({key, value, clock: k.clock, w: tabId});
            persistKey(key, value, k.clock, tabId);
        }
        dirty.clear();
        if (updates.length) post({t: "state", id: tabId, updates});
    }

    function accept(k, clock, writer) {
        return clock > k.clock || (clock === k.clock && writer > k.lastWriter);
    }

    function applyRemote(k, value, clock, writer) {
        k.clock = clock > k.clock ? clock : k.clock;   // Lamport max
        k.lastWriter = writer;
        k.hasState = true;
        k.applying = true;
        try {
            k.sig.set(value);
        }                      // synchronous → echo guard holds
        finally {
            k.applying = false;
        }
        persistKey(k.key, value, clock, writer);
    }

    function onMessage(ev) {
        const msg = ev.data;
        if (!msg || msg.id === tabId) return;          // never our own (spec excludes sender; belt-and-braces)
        rememberPeer(msg.id);

        switch (msg.t) {
            case "join":
                return;                       // presence handled by rememberPeer
            case "leave":
                forgetPeer(msg.id);
                return;
            case "req": {                              // snapshot request from a cold-starting tab
                if (keys.size === 0) return;
                const updates = [];
                for (const [key, k] of keys) updates.push({key, value: k.sig.peek(), clock: k.clock, w: k.lastWriter});
                post({t: "state", id: tabId, snapshot: true, updates});
                return;
            }
            case "state": {
                let applied = false;
                batch(() => {
                    for (const u of msg.updates) {
                        const k = keys.get(u.key);
                        if (!k) continue;
                        // A cold-starting tab adopts a snapshot unconditionally (it has
                        // nothing yet); everything else obeys the strict LWW order.
                        if ((msg.snapshot && !k.hasState) || accept(k, u.clock, u.w)) {
                            applyRemote(k, u.value, u.clock, u.w);
                            applied = true;
                        }
                    }
                });
                if (applied && status.peek() === "connecting") status.set("synced");
                return;
            }
        }
    }

    channel.addEventListener("message", onMessage);

    // Announce presence immediately. (Snapshot requests are sent per-key in sync().)
    post({t: "join", id: tabId});

    const readyTimer = setTimeout(() => {
        if (status.peek() === "connecting") status.set("synced"); // we're alone / authoritative
    }, readyMs);

    let hbTimer = null;
    if (heartbeatMs > 0) {
        hbTimer = setInterval(() => {
            post({t: "join", id: tabId});
            const cutoff = Date.now() - evictMs;
            let changed = false;
            for (const [id, seen] of lastSeen) {
                if (seen < cutoff) {
                    lastSeen.delete(id);
                    changed = true;
                }
            }
            if (changed) {
                const next = [tabId, ...lastSeen.keys()];
                next.sort();
                members.set(next);
            }
        }, heartbeatMs);
    }

    const onUnload = () => {
        try {
            post({t: "leave", id: tabId});
        } catch { /* closing */
        }
    };
    const win = typeof window !== "undefined" && window.addEventListener ? window : null;
    if (win) {
        win.addEventListener("pagehide", onUnload);
        win.addEventListener("beforeunload", onUnload);
    }

    /**
     * Bind a writable signal to a key on this channel.
     * @param {any} sig A lite-signal writable signal.
     * @param {string} [key="default"] Routing key; unique per channel.
     * @returns {{ dispose: () => void }}
     */
    function sync(sig, key = "default") {
        if (keys.has(key)) throw new Error('lite-channel: key "' + key + '" already synced on "' + channelName + '"');
        const k = {key, sig, clock: 0, lastWriter: "", hasState: false, primed: false, applying: false, stopSub: null};

        // Lone-tab cold-start fallback: hydrate from storage before wiring up.
        if (hasStorage) {
            try {
                const raw = localStorage.getItem(storageKey(key));
                if (raw) {
                    const snap = JSON.parse(raw);
                    k.clock = snap.clock | 0;
                    k.lastWriter = snap.w || "";
                    k.hasState = true;
                    k.applying = true;
                    try {
                        sig.set(snap.value);
                    } finally {
                        k.applying = false;
                    }
                }
            } catch (err) {
                onError(err);
            }
        }

        k.stopSub = sig.subscribe(() => {
            if (!k.primed) {
                k.primed = true;
                return;
            } // swallow subscribe's immediate fire
            if (k.applying) return;                     // remote-driven write: do not echo
            dirty.add(key);
            scheduleFlush();
        });
        keys.set(key, k);

        // Ask peers for the latest value of this key (cold start).
        post({t: "req", id: tabId});

        return {
            dispose() {
                if (k.stopSub) {
                    k.stopSub();
                    k.stopSub = null;
                }
                keys.delete(key);
                dirty.delete(key);
            },
        };
    }

    let disposed = false;

    function dispose() {
        if (disposed) return;
        disposed = true;
        onUnload();
        for (const [, k] of keys) {
            if (k.stopSub) k.stopSub();
        }
        keys.clear();
        dirty.clear();
        clearTimeout(readyTimer);
        if (hbTimer) clearInterval(hbTimer);
        channel.removeEventListener("message", onMessage);
        if (win) {
            win.removeEventListener("pagehide", onUnload);
            win.removeEventListener("beforeunload", onUnload);
        }
        channel.close();
    }

    return {
        channelName,
        tabId,
        sync,
        peers: readonly(peers),
        status: readonly(status),
        isLeader: readonly(isLeader),
        members: readonly(members),
        dispose,
    };
}

/**
 * Convenience wrapper: sync a single signal across tabs.
 * Returns the channel's reactive presence signals plus a combined dispose.
 *
 * @param {any} sig A lite-signal writable signal.
 * @param {string} channelName Origin-scoped channel name.
 * @param {object} [options] Same options as {@link createTabSync}.
 * @returns {{ dispose: () => void, peers: any, status: any, isLeader: any, members: any }}
 */
export function syncSignal(sig, channelName, options = {}) {
    const tab = createTabSync(channelName, options);
    const handle = tab.sync(sig, "default");
    let disposed = false;
    return {
        dispose() {
            if (disposed) return;
            disposed = true;
            handle.dispose();
            tab.dispose();
        },
        peers: tab.peers,
        status: tab.status,
        isLeader: tab.isLeader,
        members: tab.members,
    };
}
