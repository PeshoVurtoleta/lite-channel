/**
 * lite-channel protocol suite.
 *   npm test          (node --test test/*.test.js)
 *
 * Determinism: a synchronous in-process BroadcastChannel mock (harness.js) plus a
 * synchronous flush scheduler mean every assertion runs after the cluster has
 * converged — no wall-clock waits. Presence timers are exercised with node:test
 * fake timers in the one eviction test.
 */

import { test, beforeEach, mock } from "node:test";
import assert from "node:assert/strict";
import { signal } from "@zakkster/lite-signal";
import {
    installMockBC, resetBC, MockBroadcastChannel, pause, flush,
    installLocalStorage, uninstallLocalStorage, syncSchedule,
} from "../harness.js";

installMockBC();
const { createTabSync, syncSignal } = await import("../Channel.js");

const base = { schedule: syncSchedule, persist: false, heartbeatMs: 0 };
let n = 0;
const name = () => "ch-" + (++n);

beforeEach(() => resetBC());

// ── Propagation + echo prevention ───────────────────────────────────────────

test("a local write propagates to other tabs", () => {
    const ch = name();
    const A = createTabSync(ch, base), B = createTabSync(ch, base);
    const a = signal(0), b = signal(0);
    A.sync(a, "v"); B.sync(b, "v");
    a.set(5);
    assert.equal(b(), 5, "B received A's value");
    A.dispose(); B.dispose();
});

test("no echo: a remote-applied value is not re-broadcast back", () => {
    const ch = name();
    const A = createTabSync(ch, base), B = createTabSync(ch, base);
    const a = signal(0), b = signal(0);
    A.sync(a, "v"); B.sync(b, "v");

    let aChanges = 0, bChanges = 0;
    a.subscribe(() => aChanges++); // fires once immediately
    b.subscribe(() => bChanges++); // fires once immediately

    a.set(5);
    assert.equal(a(), 5);
    assert.equal(b(), 5);
    assert.equal(aChanges, 2, "A changed once (the local set), no echo back");
    assert.equal(bChanges, 2, "B changed once (the remote apply)");
    A.dispose(); B.dispose();
});

// ── Conflict resolution: the original divergence bug ────────────────────────

test("concurrent writes converge deterministically (Lamport + tabId tiebreak)", () => {
    const ch = name();
    const A = createTabSync(ch, base), B = createTabSync(ch, base);
    const a = signal(0), b = signal(0);
    A.sync(a, "v"); B.sync(b, "v");

    // Simulate a true race: both write before either sees the other.
    pause();
    a.set(1);   // tab A → state{clock:1, w:A}
    b.set(2);   // tab B → state{clock:1, w:B}
    flush();    // both delivered together

    const winnerValue = A.tabId > B.tabId ? 1 : 2; // higher id wins the equal-clock tie
    assert.equal(a(), winnerValue, "A converged to the winner");
    assert.equal(b(), winnerValue, "B converged to the winner");
    assert.equal(a(), b(), "no divergence");
    A.dispose(); B.dispose();
});

test("a strictly newer clock always wins regardless of id", () => {
    const ch = name();
    const A = createTabSync(ch, base), B = createTabSync(ch, base);
    const a = signal(0), b = signal(0);
    A.sync(a, "v"); B.sync(b, "v");
    a.set(1);          // clock 1 (A)
    b.set(2);          // B saw clock 1 → its write is clock 2 → newer
    assert.equal(a(), 2, "A accepted B's strictly-newer write");
    assert.equal(b(), 2);
    A.dispose(); B.dispose();
});

// ── Cold-start handshake ────────────────────────────────────────────────────

test("a late-joining tab gets current state via the req/snapshot handshake", () => {
    const ch = name();
    const A = createTabSync(ch, base);
    const a = signal(0);
    A.sync(a, "v");
    a.set(7);

    const B = createTabSync(ch, base);
    const b = signal(0);
    B.sync(b, "v");           // posts req → A replies snapshot
    assert.equal(b(), 7, "late tab caught up to 7");
    A.dispose(); B.dispose();
});

// ── Multiplexing ─────────────────────────────────────────────────────────────

test("multiple keys on one channel stay independent", () => {
    const ch = name();
    const A = createTabSync(ch, base), B = createTabSync(ch, base);
    const a1 = signal(0), a2 = signal(0), b1 = signal(0), b2 = signal(0);
    A.sync(a1, "k1"); A.sync(a2, "k2");
    B.sync(b1, "k1"); B.sync(b2, "k2");
    a1.set(11); a2.set(22);
    assert.equal(b1(), 11);
    assert.equal(b2(), 22);
    A.dispose(); B.dispose();
});

test("syncing a duplicate key throws", () => {
    const ch = name();
    const A = createTabSync(ch, base);
    A.sync(signal(0), "dup");
    assert.throws(() => A.sync(signal(1), "dup"), /already synced/);
    A.dispose();
});

// ── Reactive presence ───────────────────────────────────────────────────────

test("peers reflects tabs joining and leaving", () => {
    const ch = name();
    const A = createTabSync(ch, base);
    assert.equal(A.peers(), 0, "alone");
    const B = createTabSync(ch, base);
    assert.equal(A.peers(), 1, "B joined");
    assert.equal(B.peers(), 1, "B sees A (via re-announce)");
    B.dispose();
    assert.equal(A.peers(), 0, "B left");
    A.dispose();
});

test("isLeader is deterministic (lowest tabId leads)", () => {
    const ch = name();
    const A = createTabSync(ch, base), B = createTabSync(ch, base);
    const aLeads = A.tabId < B.tabId;
    assert.equal(A.isLeader(), aLeads);
    assert.equal(B.isLeader(), !aLeads);
    // exactly one leader
    assert.equal(Number(A.isLeader()) + Number(B.isLeader()), 1);
    A.dispose(); B.dispose();
});

test("status flips connecting -> synced when a snapshot is applied", () => {
    const ch = name();
    const A = createTabSync(ch, base);
    A.sync(signal(7), "v");
    const B = createTabSync(ch, base);
    assert.equal(B.status(), "connecting");
    const b = signal(0);
    B.sync(b, "v");                         // snapshot arrives synchronously
    assert.equal(B.status(), "synced");
    assert.equal(b(), 7, "seeded (never-written) state transfers on cold start");
    A.dispose(); B.dispose();
});

// ── Persistence (lone-tab cold start) ───────────────────────────────────────

test("a value persists to localStorage and rehydrates a fresh lone tab", () => {
    installLocalStorage();
    try {
        const ch = name();
        const A = createTabSync(ch, { schedule: syncSchedule, persist: true, heartbeatMs: 0 });
        const a = signal(0);
        A.sync(a, "v");
        a.set(9);
        A.dispose();                        // tab closes; storage retains the value

        const C = createTabSync(ch, { schedule: syncSchedule, persist: true, heartbeatMs: 0 });
        const c = signal(0);
        C.sync(c, "v");                     // no peers → hydrate from storage
        assert.equal(c(), 9, "rehydrated from localStorage");
        C.dispose();
    } finally {
        uninstallLocalStorage();
    }
});

// ── Lifecycle ────────────────────────────────────────────────────────────────

test("dispose is idempotent and severs sync", () => {
    const ch = name();
    const A = createTabSync(ch, base), B = createTabSync(ch, base);
    const a = signal(0), b = signal(0);
    A.sync(a, "v"); B.sync(b, "v");
    B.dispose();
    assert.doesNotThrow(() => { B.dispose(); B.dispose(); });
    a.set(42);
    assert.equal(b(), 0, "disposed tab no longer receives updates");
    A.dispose();
});

test("syncSignal convenience wrapper round-trips", () => {
    const ch = name();
    const a = signal(0), b = signal(0);
    const ha = syncSignal(a, ch, base);
    const hb = syncSignal(b, ch, base);
    a.set(3);
    assert.equal(b(), 3);
    assert.equal(typeof ha.peers, "function");
    assert.equal(ha.peers(), 1);
    ha.dispose(); hb.dispose();
});

// ── Error isolation ─────────────────────────────────────────────────────────

test("a non-cloneable value routes to onError without crashing", () => {
    const ch = name();
    const onError = mock.fn();
    const A = createTabSync(ch, { schedule: syncSchedule, persist: false, heartbeatMs: 0, onError });
    const a = signal(0);
    A.sync(a, "v");
    assert.doesNotThrow(() => a.set(() => {})); // functions are not structured-cloneable
    assert.ok(onError.mock.callCount() >= 1, "error surfaced");
    A.dispose();
});

// ── Eviction (fake timers) ──────────────────────────────────────────────────

test("a silent peer is evicted after evictMs", () => {
    mock.timers.enable({ apis: ["setInterval", "setTimeout", "Date"] });
    try {
        const ch = name();
        const A = createTabSync(ch, { schedule: syncSchedule, persist: false, heartbeatMs: 1000, evictMs: 2500 });
        const raw = new MockBroadcastChannel(ch);
        raw.postMessage({ t: "join", id: "ghost-peer" });   // A learns a peer at t=0
        assert.equal(A.peers(), 1);
        mock.timers.tick(3000);                              // peer never heartbeats again
        assert.equal(A.peers(), 0, "stale peer evicted");
        A.dispose(); raw.close();
    } finally {
        mock.timers.reset();
    }
});
