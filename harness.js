/**
 * Deterministic test harness for lite-channel.
 *
 * `MockBroadcastChannel` models the spec behaviour that matters for the protocol:
 * a message posted on a channel is delivered to every OTHER open channel of the
 * same name (never the sender). Delivery is synchronous but re-entrancy-safe via
 * a drain queue, so once `postMessage` (and any cascade it triggers) settles, the
 * cluster is in its converged state — exactly what we assert.
 *
 * For true concurrency (two tabs writing "at the same time"), call `pause()`,
 * issue the writes, then `flush()`: messages queue while paused and are all
 * delivered in one drain, so neither tab sees the other's write first.
 *
 * `installLocalStorage()` provides a minimal localStorage for persistence tests.
 */

const hub = new Map();        // name -> Set<MockBroadcastChannel>
let queue = [];
let draining = false;
let paused = false;

export class MockBroadcastChannel {
    constructor(name) {
        this.name = name;
        this._listeners = new Set();
        this.onmessage = null;
        this.closed = false;
        if (!hub.has(name)) hub.set(name, new Set());
        hub.get(name).add(this);
    }
    addEventListener(type, fn) { if (type === "message") this._listeners.add(fn); }
    removeEventListener(type, fn) { if (type === "message") this._listeners.delete(fn); }
    postMessage(data) {
        if (this.closed) return;
        // structuredClone mirrors real transfer semantics — and throws on
        // non-cloneable payloads (functions, etc.), which the lib must handle.
        const cloned = structuredClone(data);
        queue.push([this, cloned]);
        if (!paused) drain();
    }
    close() {
        this.closed = true;
        this._listeners.clear();
        const set = hub.get(this.name);
        if (set) set.delete(this);
    }
    _deliver(data) {
        const ev = { data };
        if (typeof this.onmessage === "function") this.onmessage(ev);
        for (const fn of this._listeners) fn(ev);
    }
}

function drain() {
    if (draining) return;
    draining = true;
    while (queue.length) {
        const [from, data] = queue.shift();
        const set = hub.get(from.name);
        if (!set) continue;
        for (const ch of set) {
            if (ch !== from && !ch.closed) ch._deliver(data);
        }
    }
    draining = false;
}

/** Buffer all posted messages until flush() (simulates simultaneous delivery). */
export function pause() { paused = true; }
/** Deliver everything buffered since pause(). */
export function flush() { paused = false; drain(); }

export function installMockBC() {
    globalThis.__realBC = globalThis.BroadcastChannel;
    globalThis.BroadcastChannel = MockBroadcastChannel;
}
export function uninstallMockBC() {
    globalThis.BroadcastChannel = globalThis.__realBC;
    resetBC();
}
export function resetBC() {
    hub.clear();
    queue = [];
    draining = false;
    paused = false;
}

// ── localStorage mock ───────────────────────────────────────────────────────
export function installLocalStorage() {
    const store = new Map();
    globalThis.localStorage = {
        getItem: (k) => (store.has(k) ? store.get(k) : null),
        setItem: (k, v) => { store.set(k, String(v)); },
        removeItem: (k) => { store.delete(k); },
        clear: () => { store.clear(); },
        get length() { return store.size; },
        key: (i) => [...store.keys()][i] ?? null,
    };
    return store;
}
export function uninstallLocalStorage() { delete globalThis.localStorage; }

/** Synchronous flush scheduler for tests (no microtask deferral). */
export const syncSchedule = (f) => f();
