package org.apache.ignite.internal.processors.query.calcite.exec;

import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class InboxController {

    public enum SourceControlType {
        SPLITTER,
        DUPLICATOR,

    }

    /**
     * The sub-inboxes this controller is responsible for
     */
    protected final Map<Long, Inbox<?>> inboxes;

    /**
     * Countdown latch for the inbox loading
     */
    protected final DelayedCountDownLatch inboxesRemaining;

    /**
     * Countdown latch for the controller initialization. Up to subclasses to use it properly.
     */
    protected final CountDownLatch controllerInitialized;

    /**  */
    private final int requiredInboxes;

    /**
     * Default constructor
     */
    protected InboxController(int requiredInboxes) {
        this.requiredInboxes = requiredInboxes;
        this.inboxes = new ConcurrentHashMap<>();
        this.inboxesRemaining = new DelayedCountDownLatch();
        this.controllerInitialized = new CountDownLatch(1);
    }

    /**
     * Creates a new InboxController copying over the references from a partial one
     *
     * @param partial The inbox controller to copy references from
     */
    private InboxController(InboxController partial) {
        this.requiredInboxes = partial.requiredInboxes;
        this.inboxesRemaining = partial.inboxesRemaining;
        this.inboxes = partial.inboxes;
        this.controllerInitialized = partial.controllerInitialized;
    }

    /**
     * Unregisters an inbox
     *
     * @return true if there are no more inboxes, false otherwise
     */
    public boolean unregister(long fragmentId) {
        inboxes.remove(fragmentId);
        return inboxes.size() == 0;
    }

    /**
     * Returns a collection of all inboxes registered
     */
    public Collection<Inbox<?>> allInboxes() {
        return inboxes.values();
    }

    /**
     * Add a sub-inbox to the controller if it does not already have a mapping to the fragmentId
     *
     * @param inboxFragmentId Fragment to map too. This should be unique to the inbox instance
     * @param inbox           Inbox to add
     * @returns The inbox that was mapped to fragmentId, or null if there was none. Similar to {@link java.util.Map#putIfAbsent}
     */
    @Nullable Inbox<?> addIfAbsent(long inboxFragmentId, Inbox<?> inbox) {
        assert !ready();

        Inbox<?> old = inboxes.putIfAbsent(inboxFragmentId, inbox);
        if (old == null) inboxesRemaining.countDown();
        return old;
    }

    /**
     * Node left handler
     */
    void onNodeLeft(UUID nodeId) {
        inboxes.values().forEach(inbox -> inbox.context().execute(() -> inbox.onNodeLeft(nodeId), inbox::onError));
    }

    /**
     * Close the inbox controller, cascading close calls to its inboxes
     */
    void close() {
        inboxes.values().forEach(inbox -> inbox.context().execute(inbox::close, inbox::onError));
    }


    /**
     * Throws an exception in the context of each inbox.
     */
    void error(Exception e) {
        inboxes.values().forEach(inbox -> inbox.context().execute(() -> { throw e; }, inbox::onError));
    }

    /**
     * Returns a boolean indicating the ready status, possibly waiting up to the specified {@code 2*timeout}
     * ({@code 1*timeout} for the inboxes to be ready and {@code 1*timeout} for the controller to initialize)
     *
     * @return true if ready, false if timeout expired.
     */
    boolean waitForReady(long timeout, TimeUnit unit) throws InterruptedException {
        if (!inboxesRemaining.await(timeout, unit)) return false;
        return controllerInitialized.await(timeout, unit);
    }

    /**
     * Checks if the controller has been initialized. Should be true unless we are in an UnknownController instance
     */
    boolean initialized() {
        return controllerInitialized.getCount() == 0;
    }

    /**
     * Whether the inbox controller is ready to process inputs
     */
    abstract boolean ready();

    /**
     * Processes a query batch message, distributing the contents to its child inboxes
     *
     * @param node    The sending node
     * @param message The batch message
     */
    abstract void processBatch(UUID node, QueryBatchMessage message);

    static class UnknownController extends InboxController {

        public UnknownController() {
            super(-1);
        }

        /** {@inheritDoc} */
        @Override
        public boolean ready() {
            return false;
        }

        /** {@inheritDoc} */
        @Override
        public void processBatch(UUID node, QueryBatchMessage message) {
            throw new UnsupportedOperationException();
        }
    }

    static class DuplicatorController extends InboxController {

        DuplicatorController(int requiredInboxes) {
            super(requiredInboxes);
            init(requiredInboxes);
        }

        DuplicatorController(InboxController partial, int requiredInboxes) {
            super(partial);
            init(requiredInboxes);
        }

        private void init(int requiredInboxes) {
            inboxesRemaining.init(requiredInboxes);
            controllerInitialized.countDown();
        }

        /** {@inheritDoc} */
        @Override
        boolean ready() {
            return inboxesRemaining.getCount() == 0 && controllerInitialized.getCount() == 0;
        }

        /** {@inheritDoc} */
        @Override
        public void processBatch(UUID nodeId, QueryBatchMessage msg) {
            for (Inbox<?> inbox : inboxes.values())
                inbox.context().execute(() -> {
                    inbox.onBatchReceived(nodeId, msg.outboxFragmentId(), msg.batchId(), msg.last(), Commons.cast(msg.rows()));
                }, inbox::onError);
        }

    }

    static class SplitterController extends InboxController {

        private int currentInbox = -1;
        private int totalInboxes;

        private final List<Inbox<?>> inboxList = new ArrayList<>();

        SplitterController(int requiredInboxes) {
            super(requiredInboxes);
            init(requiredInboxes);
        }

        SplitterController(InboxController partial, int requiredInboxes) {
            super(partial);
            init(requiredInboxes);
        }

        private void init(int requiredInboxes) {
            // Register twice, once in the hashmap and once in the inboxList array
            inboxesRemaining.init(2 * requiredInboxes);
            controllerInitialized.countDown();
            totalInboxes = requiredInboxes;
        }

        /** {@inheritDoc} */
        @Override
        @Nullable Inbox<?> addIfAbsent(long inboxFragmentId, Inbox<?> inbox) {
            Inbox<?> alreadyRegistered = super.addIfAbsent(inboxFragmentId, inbox);
            if (alreadyRegistered == null) synchronized (inboxList) {
                inboxList.add(inbox);
            }
            inboxesRemaining.countDown();
            return alreadyRegistered;
        }

        /** {@inheritDoc} */
        @Override
        boolean ready() {
            return inboxesRemaining.getCount() == 0 && controllerInitialized.getCount() == 0;
        }

        /** {@inheritDoc} */
        @Override
        public void processBatch(UUID nodeId, QueryBatchMessage msg) {
            assert inboxList.size() == totalInboxes;
            currentInbox = ++currentInbox % totalInboxes;
            QueryBatchMessage emptyMessage = msg.cloneWithNoRows();

            for (int i = 0; i < totalInboxes; i++) {
                if (i == currentInbox) process(inboxList.get(i), nodeId, msg);
                else process(inboxList.get(i), nodeId, emptyMessage);
            }
        }

        private void process(Inbox<?> inbox, UUID nodeId, QueryBatchMessage msg) {
            inbox.context().execute(
                () -> inbox.onBatchReceived(nodeId, msg.outboxFragmentId(), msg.batchId(), msg.last(), Commons.cast(msg.rows())),
                inbox::onError
            );
        }

    }
}
