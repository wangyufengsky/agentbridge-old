package com.github.catatafishen.agentbridge.services;

import com.github.catatafishen.agentbridge.psi.PlatformApiCompat;
import com.intellij.openapi.components.Service;
import com.intellij.openapi.diagnostic.Logger;
import com.intellij.openapi.project.Project;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks in-flight MCP tool work by MCP transport session.
 *
 * <p>The registry is project-scoped because the MCP server is project-scoped, but cancellation
 * is deliberately <em>not</em> project-scoped. An IDE chat agent and an external MCP client can
 * call the same project server concurrently. Stopping one AI session must therefore cancel only
 * the MCP transport session associated with that AI session; otherwise unrelated clients can
 * inherit a stale cancellation latch and fail before their tool starts.
 */
@SuppressWarnings("unused") // Used via PlatformApiCompat.getService — IDE doesn't see reflective lookup
@Service(Service.Level.PROJECT)
public final class InFlightMcpToolRegistry {

    private static final Logger LOG = Logger.getInstance(InFlightMcpToolRegistry.class);

    private record FutureRegistration(
        @NotNull String transportSessionKey,
        @NotNull CompletableFuture<String> future
    ) {
    }

    private final Map<String, FutureRegistration> inFlight = new ConcurrentHashMap<>();
    private final Map<Thread, String> workers = new ConcurrentHashMap<>();

    /** AI/ACP session ID -> MCP transport session keys (reconnects/sub-agents may use several). */
    private final Map<String, Set<String>> transportsByAgentSession = new ConcurrentHashMap<>();
    /** MCP transport session key -> AI/ACP session ID. */
    private final Map<String, String> agentByTransportSession = new ConcurrentHashMap<>();

    /** Latched AI-session cancellations. Cleared only when that same AI session starts a new turn. */
    private final Map<String, String> cancelledAgentSessions = new ConcurrentHashMap<>();
    /** Latched transport closures. MCP session IDs are unique, so a closed key is never reused. */
    private final Map<String, String> closedTransportSessions = new ConcurrentHashMap<>();

    /** Server-wide shutdown latch. This is never used for stopping an individual AI client. */
    private final AtomicBoolean serverStopped = new AtomicBoolean(false);
    private volatile String serverStopReason = "MCP server stopped";

    @SuppressWarnings("unused") // Project parameter required by IntelliJ project-level service contract
    public InFlightMcpToolRegistry(@NotNull Project project) {
    }

    public static InFlightMcpToolRegistry getInstance(@NotNull Project project) {
        return PlatformApiCompat.getService(project, InFlightMcpToolRegistry.class);
    }

    /**
     * Associates an MCP transport session with the AI session whose ACP tool event correlated
     * with the MCP call. If that AI session was already cancelled, this closes the late-binding
     * race by immediately cancelling work already registered on the transport session.
     */
    public void associateAgentSession(
        @NotNull String agentSessionId,
        @NotNull String transportSessionKey
    ) {
        String previousAgent = agentByTransportSession.put(transportSessionKey, agentSessionId);
        if (previousAgent != null && !previousAgent.equals(agentSessionId)) {
            removeAgentTransportAssociation(previousAgent, transportSessionKey);
        }
        transportsByAgentSession
            .computeIfAbsent(agentSessionId, ignored -> ConcurrentHashMap.newKeySet())
            .add(transportSessionKey);

        String reason = cancelledAgentSessions.get(agentSessionId);
        if (reason != null) {
            cancelTransportWork(transportSessionKey, reason);
        }
    }

    /**
     * Register a future under the current MCP transport session. The caller must unregister it
     * in a {@code finally} block.
     */
    public void register(@NotNull String id, @NotNull CompletableFuture<String> future) {
        register(McpCallContext.currentOrFallback(), id, future);
    }

    public void register(
        @NotNull String transportSessionKey,
        @NotNull String id,
        @NotNull CompletableFuture<String> future
    ) {
        String cancellationReason = cancellationReason(transportSessionKey);
        if (cancellationReason != null) {
            cancelFuture(future, cancellationReason);
            return;
        }

        inFlight.put(id, new FutureRegistration(transportSessionKey, future));
        // Re-check after insertion to close races with agent/session/server cancellation.
        cancellationReason = cancellationReason(transportSessionKey);
        if (cancellationReason != null) {
            FutureRegistration registration = inFlight.remove(id);
            if (registration != null) {
                cancelFuture(registration.future(), cancellationReason);
            }
        }
    }

    public void unregister(@NotNull String id) {
        inFlight.remove(id);
    }

    /**
     * Register the worker thread executing a tool under its MCP transport session.
     */
    public void registerWorker(
        @NotNull String transportSessionKey,
        @NotNull Thread worker
    ) {
        workers.put(worker, transportSessionKey);
        if (cancellationReason(transportSessionKey) != null) {
            worker.interrupt();
            workers.remove(worker, transportSessionKey);
        }
    }

    /**
     * Compatibility overload for direct callers and tests outside an MCP request.
     */
    public void registerWorker(@NotNull Thread worker) {
        registerWorker(McpCallContext.currentOrFallback(), worker);
    }

    public void unregisterWorker(@NotNull Thread worker) {
        workers.remove(worker);
    }

    /**
     * Cancels and latches only the MCP transport owned by one AI/ACP session. Other AI sessions
     * and external MCP clients continue unaffected.
     */
    public void cancelAgentSession(
        @NotNull String agentSessionId,
        @NotNull String reason
    ) {
        cancelledAgentSessions.putIfAbsent(agentSessionId, reason);
        for (String transportSessionKey :
            Set.copyOf(transportsByAgentSession.getOrDefault(agentSessionId, Set.of()))) {
            cancelTransportWork(transportSessionKey, reason);
        }
    }

    /**
     * Reopens one AI session for its next turn without changing any other session's state.
     */
    public void reopenAgentSession(@NotNull String agentSessionId) {
        if (cancelledAgentSessions.remove(agentSessionId) != null) {
            LOG.info("InFlightMcpToolRegistry: re-opened AI session " + agentSessionId);
        }
    }

    /**
     * Closes one MCP transport session and releases its work. A closed transport key stays
     * latched so a request racing with DELETE/disconnect cannot register new work afterward.
     */
    public void closeTransportSession(
        @NotNull String transportSessionKey,
        @NotNull String reason
    ) {
        closedTransportSessions.putIfAbsent(transportSessionKey, reason);
        cancelTransportWork(transportSessionKey, reason);

        String agentSessionId = agentByTransportSession.remove(transportSessionKey);
        if (agentSessionId != null) {
            removeAgentTransportAssociation(agentSessionId, transportSessionKey);
        }
    }

    /**
     * Server lifecycle cancellation. Individual agent stop paths must use
     * {@link #cancelAgentSession(String, String)} instead.
     */
    public void cancelAll(@NotNull String reason) {
        serverStopReason = reason;
        serverStopped.set(true);
        int futures = drainFutures(null, reason);
        int interrupted = interruptWorkers(null);
        if (futures > 0 || interrupted > 0) {
            LOG.info("InFlightMcpToolRegistry: server stop cancelled " + futures
                + " future(s) and interrupted " + interrupted + " worker(s) — " + reason);
        }
    }

    /**
     * Reopens the server-wide lifecycle latch when the MCP server is started again.
     */
    public void reopenAll() {
        if (serverStopped.compareAndSet(true, false)) {
            serverStopReason = "MCP server stopped";
            LOG.info("InFlightMcpToolRegistry: re-opened after MCP server restart");
        }
    }

    private @Nullable String cancellationReason(
        @NotNull String transportSessionKey
    ) {
        if (serverStopped.get()) return serverStopReason;

        String reason = closedTransportSessions.get(transportSessionKey);
        if (reason != null) return reason;

        String agentSessionId = agentByTransportSession.get(transportSessionKey);
        return agentSessionId != null ? cancelledAgentSessions.get(agentSessionId) : null;
    }

    private void cancelTransportWork(
        @NotNull String transportSessionKey,
        @NotNull String reason
    ) {
        int futures = drainFutures(transportSessionKey, reason);
        int interrupted = interruptWorkers(transportSessionKey);
        if (futures > 0 || interrupted > 0) {
            LOG.info("InFlightMcpToolRegistry: cancelled transport " + transportSessionKey
                + " (" + futures + " future(s), " + interrupted + " worker(s)) — " + reason);
        }
    }

    private void removeAgentTransportAssociation(
        @NotNull String agentSessionId,
        @NotNull String transportSessionKey
    ) {
        Set<String> transports = transportsByAgentSession.get(agentSessionId);
        if (transports == null) return;
        transports.remove(transportSessionKey);
        if (transports.isEmpty()) {
            transportsByAgentSession.remove(agentSessionId, transports);
        }
    }

    private int drainFutures(
        @Nullable String transportSessionKey,
        @NotNull String reason
    ) {
        int count = 0;
        for (Map.Entry<String, FutureRegistration> entry : Map.copyOf(inFlight).entrySet()) {
            FutureRegistration registration = entry.getValue();
            if (transportSessionKey != null
                && !transportSessionKey.equals(registration.transportSessionKey())) {
                continue;
            }
            if (inFlight.remove(entry.getKey(), registration)) {
                cancelFuture(registration.future(), reason);
                count++;
            }
        }
        return count;
    }

    private int interruptWorkers(@Nullable String transportSessionKey) {
        int count = 0;
        for (Map.Entry<Thread, String> entry : Map.copyOf(workers).entrySet()) {
            if (transportSessionKey != null && !transportSessionKey.equals(entry.getValue())) {
                continue;
            }
            if (workers.remove(entry.getKey(), entry.getValue())) {
                entry.getKey().interrupt();
                count++;
            }
        }
        return count;
    }

    private static void cancelFuture(
        @NotNull CompletableFuture<String> future,
        @NotNull String reason
    ) {
        if (!future.isDone()) {
            future.completeExceptionally(new CancellationException(reason));
        }
    }
}
