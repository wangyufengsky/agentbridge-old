package com.github.catatafishen.agentbridge.services;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Serializes prompt admission at the HTTP boundary so two callers cannot start overlapping turns.
 */
final class PromptAdmissionGate {

    private final AtomicBoolean busy = new AtomicBoolean();

    boolean tryReserve() {
        return busy.compareAndSet(false, true);
    }

    void setBusy(boolean value) {
        busy.set(value);
    }

    void release() {
        busy.set(false);
    }

    boolean isBusy() {
        return busy.get();
    }
}
