/**
 * @zakkster/lite-channel — type declarations
 * Cross-tab synchronization for @zakkster/lite-signal over BroadcastChannel.
 */

/** A read-only reactive accessor (callable + peek + subscribe). */
export interface ReadonlySignal<T> {
    (): T;
    peek(): T;
    subscribe(fn: (value: T) => void): () => void;
}

/** Minimal structural shape of a lite-signal writable signal. */
export interface WritableSignal<T> {
    (): T;
    peek(): T;
    set(value: T): void;
    update(fn: (prev: T) => void): void;
    subscribe(fn: (value: T) => void): () => void;
}

export type SyncStatus = "connecting" | "synced";

export interface TabSyncOptions {
    /** Outbound flush scheduler. Default `(f) => queueMicrotask(f)`. Use `(f) => f()` to flush synchronously. */
    schedule?: (flush: () => void) => void;
    /** Mirror each key to localStorage for lone-tab cold start. Default `true`. Values must be JSON-serialisable. */
    persist?: boolean;
    /** Presence re-announce interval (ms). `0` disables the heartbeat. Default `2000`. */
    heartbeatMs?: number;
    /** Drop a peer not heard from within this window (ms). Default `5000`. */
    evictMs?: number;
    /** If no snapshot arrives within this window (ms), assume first tab and flip status to "synced". Default `150`. */
    readyMs?: number;
    /** Error sink for clone/storage failures. Default logs to console. */
    onError?: (err: unknown) => void;
}

/** Handle for a single key bound to a channel. */
export interface SyncHandle {
    dispose(): void;
}

/** A multiplexed cross-tab sync bus over one BroadcastChannel. */
export interface TabSync {
    readonly channelName: string;
    readonly tabId: string;
    /** Bind a writable signal to a key (unique per channel; defaults to "default"). */
    sync<T>(sig: WritableSignal<T>, key?: string): SyncHandle;
    /** Number of other live tabs on this channel. */
    peers: ReadonlySignal<number>;
    /** "connecting" until first snapshot/readiness, then "synced". */
    status: ReadonlySignal<SyncStatus>;
    /** True in exactly one tab (lowest tabId) among the live set. */
    isLeader: ReadonlySignal<boolean>;
    /** Sorted live tab ids, including this tab. */
    members: ReadonlySignal<string[]>;
    dispose(): void;
}

/** Create a multiplexed tab-sync bus over a single BroadcastChannel. */
export declare function createTabSync(channelName: string, options?: TabSyncOptions): TabSync;

/** Convenience wrapper: sync a single signal across tabs. */
export declare function syncSignal<T>(
    sig: WritableSignal<T>,
    channelName: string,
    options?: TabSyncOptions,
): {
    dispose(): void;
    peers: ReadonlySignal<number>;
    status: ReadonlySignal<SyncStatus>;
    isLeader: ReadonlySignal<boolean>;
    members: ReadonlySignal<string[]>;
};
