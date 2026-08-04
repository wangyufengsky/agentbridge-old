package com.github.catatafishen.agentbridge.services;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PromptAdmissionGateTest {

    @Test
    void rejectsASecondReservationUntilTheCurrentTurnIsReleased() {
        PromptAdmissionGate gate = new PromptAdmissionGate();

        assertTrue(gate.tryReserve());
        assertTrue(gate.isBusy());
        assertFalse(gate.tryReserve());

        gate.release();
        assertTrue(gate.tryReserve());
    }

    @Test
    void admitsOnlyOneConcurrentCaller() throws InterruptedException {
        PromptAdmissionGate gate = new PromptAdmissionGate();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(8);
        AtomicInteger admitted = new AtomicInteger();

        for (int index = 0; index < 8; index++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    if (gate.tryReserve()) {
                        admitted.incrementAndGet();
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            thread.start();
        }

        start.countDown();
        done.await();

        assertEquals(1, admitted.get());
    }

    @Test
    void mirrorsUiSendingState() {
        PromptAdmissionGate gate = new PromptAdmissionGate();

        gate.setBusy(true);
        assertFalse(gate.tryReserve());

        gate.setBusy(false);
        assertTrue(gate.tryReserve());
    }
}
