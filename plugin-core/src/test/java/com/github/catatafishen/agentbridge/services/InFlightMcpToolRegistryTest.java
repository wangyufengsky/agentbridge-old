package com.github.catatafishen.agentbridge.services;

import com.intellij.openapi.project.Project;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class InFlightMcpToolRegistryTest {

    private static final String AI_SESSION = "agent-session-1";
    private static final String AI_TRANSPORT = "http:ai-transport";
    private static final String EXTERNAL_TRANSPORT = "http:sql-review";

    private final InFlightMcpToolRegistry registry =
        new InFlightMcpToolRegistry(mock(Project.class));

    @Test
    void cancelAgentSession_cancelsOnlyItsTransportFuture() {
        registry.associateAgentSession(AI_SESSION, AI_TRANSPORT);
        CompletableFuture<String> ai = new CompletableFuture<>();
        CompletableFuture<String> external = new CompletableFuture<>();
        registry.register(AI_TRANSPORT, "ai", ai);
        registry.register(EXTERNAL_TRANSPORT, "external", external);

        registry.cancelAgentSession(AI_SESSION, "agent stopped");

        assertCancelledWith(ai, "agent stopped");
        assertFalse(external.isDone(),
            "Stopping an IDE AI session must not cancel an external MCP client's call");
    }

    @Test
    void cancelAgentSession_cancelsEveryTransportAssociatedAfterReconnect() {
        String reconnectedTransport = "http:ai-reconnected";
        registry.associateAgentSession(AI_SESSION, AI_TRANSPORT);
        registry.associateAgentSession(AI_SESSION, reconnectedTransport);
        CompletableFuture<String> original = new CompletableFuture<>();
        CompletableFuture<String> reconnected = new CompletableFuture<>();
        registry.register(AI_TRANSPORT, "original", original);
        registry.register(reconnectedTransport, "reconnected", reconnected);

        registry.cancelAgentSession(AI_SESSION, "agent stopped");

        assertCancelledWith(original, "agent stopped");
        assertCancelledWith(reconnected, "agent stopped");
    }

    @Test
    void cancelAgentSession_interruptsOnlyItsWorker() throws Exception {
        registry.associateAgentSession(AI_SESSION, AI_TRANSPORT);
        WorkerProbe ai = startWorker(AI_TRANSPORT, "ai-worker");
        WorkerProbe external = startWorker(EXTERNAL_TRANSPORT, "external-worker");

        registry.cancelAgentSession(AI_SESSION, "agent stopped");

        assertTrue(ai.finished().await(2, TimeUnit.SECONDS));
        assertTrue(ai.interrupted().get());
        assertFalse(external.interrupted().get());
        external.thread().interrupt();
        assertTrue(external.finished().await(2, TimeUnit.SECONDS));
    }

    @Test
    void cancelledAgentSession_rejectsLateAiRegistration_butNotExternalRegistration() {
        registry.associateAgentSession(AI_SESSION, AI_TRANSPORT);
        registry.cancelAgentSession(AI_SESSION, "agent stopped");

        CompletableFuture<String> lateAi = new CompletableFuture<>();
        CompletableFuture<String> external = new CompletableFuture<>();
        registry.register(AI_TRANSPORT, "late-ai", lateAi);
        registry.register(EXTERNAL_TRANSPORT, "external", external);

        assertCancelledWith(lateAi, "agent stopped");
        assertFalse(external.isDone());
    }

    @Test
    void associationAfterCancellation_closesLateCorrelationRace() {
        registry.cancelAgentSession(AI_SESSION, "agent stopped");
        CompletableFuture<String> alreadyRunning = new CompletableFuture<>();
        registry.register(AI_TRANSPORT, "running", alreadyRunning);

        registry.associateAgentSession(AI_SESSION, AI_TRANSPORT);

        assertCancelledWith(alreadyRunning, "agent stopped");
    }

    @Test
    void reopenAgentSession_allowsOnlyThatSessionToRunAgain() {
        registry.associateAgentSession(AI_SESSION, AI_TRANSPORT);
        registry.cancelAgentSession(AI_SESSION, "stopped by user");
        registry.reopenAgentSession(AI_SESSION);

        CompletableFuture<String> nextTurn = new CompletableFuture<>();
        registry.register(AI_TRANSPORT, "next-turn", nextTurn);

        assertFalse(nextTurn.isDone());
    }

    @Test
    void closeTransportSession_doesNotAffectAnotherTransport() {
        CompletableFuture<String> closed = new CompletableFuture<>();
        CompletableFuture<String> other = new CompletableFuture<>();
        registry.register(AI_TRANSPORT, "closed", closed);
        registry.register(EXTERNAL_TRANSPORT, "other", other);

        registry.closeTransportSession(AI_TRANSPORT, "session closed");

        assertCancelledWith(closed, "session closed");
        assertFalse(other.isDone());
    }

    @Test
    void cancelAll_isReservedForServerLifecycle_andReopenAllAllowsRestart() {
        CompletableFuture<String> beforeStop = new CompletableFuture<>();
        registry.register(EXTERNAL_TRANSPORT, "before-stop", beforeStop);

        registry.cancelAll("MCP server stopped");
        assertCancelledWith(beforeStop, "MCP server stopped");

        CompletableFuture<String> whileStopped = new CompletableFuture<>();
        registry.register("http:new", "while-stopped", whileStopped);
        assertCancelledWith(whileStopped, "MCP server stopped");

        registry.reopenAll();
        CompletableFuture<String> afterRestart = new CompletableFuture<>();
        registry.register("http:new", "after-restart", afterRestart);
        assertFalse(afterRestart.isDone());
    }

    @Test
    void unregister_removesFuture() {
        CompletableFuture<String> future = new CompletableFuture<>();
        registry.register(AI_TRANSPORT, "future", future);
        registry.unregister("future");

        assertDoesNotThrow(() ->
            registry.closeTransportSession(AI_TRANSPORT, "session closed"));
        assertFalse(future.isDone());
    }

    private WorkerProbe startWorker(String transportSessionKey, String name)
        throws InterruptedException {
        CountDownLatch registered = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread worker = new Thread(() -> {
            registry.registerWorker(transportSessionKey, Thread.currentThread());
            registered.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            } finally {
                registry.unregisterWorker(Thread.currentThread());
                finished.countDown();
            }
        }, name);
        worker.start();
        assertTrue(registered.await(2, TimeUnit.SECONDS));
        return new WorkerProbe(worker, finished, interrupted);
    }

    private record WorkerProbe(
        Thread thread,
        CountDownLatch finished,
        AtomicBoolean interrupted
    ) {
    }

    private static void assertCancelledWith(
        CompletableFuture<String> future,
        String expectedReason
    ) {
        CancellationException exception = assertThrows(
            CancellationException.class,
            () -> future.get(1, TimeUnit.SECONDS));
        Throwable cause = exception.getCause();
        assertEquals(expectedReason, cause != null ? cause.getMessage() : exception.getMessage());
    }
}
