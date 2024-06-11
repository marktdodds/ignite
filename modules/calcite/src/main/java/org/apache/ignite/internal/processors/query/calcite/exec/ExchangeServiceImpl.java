/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.Query;
import org.apache.ignite.internal.processors.query.calcite.QueryRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Inbox;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Outbox;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpIoTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.NoOpMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.message.ErrorMessage;
import org.apache.ignite.internal.processors.query.calcite.message.InboxCloseMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchAcknowledgeMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryBatchMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryCloseMessage;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class ExchangeServiceImpl extends AbstractService implements ExchangeService {
    /** */
    public static final long INBOX_INITIALIZATION_TIMEOUT = 10_000L;

    /** */
    private final UUID locaNodeId;

    /** */
    private QueryTaskExecutor taskExecutor;

    /** */
    private MailboxRegistry mailboxRegistry;

    /** */
    private MessageService msgSvc;

    /** */
    private TimeoutService timeoutSvc;

    /** */
    private QueryRegistry qryRegistry;

    /**
     * @param ctx Kernal context.
     */
    public ExchangeServiceImpl(GridKernalContext ctx) {
        super(ctx);

        locaNodeId = ctx.localNodeId();
    }

    /**
     * @param taskExecutor Task executor.
     */
    public void taskExecutor(QueryTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    /**
     * @return Task executor.
     */
    public QueryTaskExecutor taskExecutor() {
        return taskExecutor;
    }

    /**
     * @param mailboxRegistry Mailbox registry.
     */
    public void mailboxRegistry(MailboxRegistry mailboxRegistry) {
        this.mailboxRegistry = mailboxRegistry;
    }

    /**
     * @return  Mailbox registry.
     */
    public MailboxRegistry mailboxRegistry() {
        return mailboxRegistry;
    }

    /**
     * @param msgSvc Message service.
     */
    public void messageService(MessageService msgSvc) {
        this.msgSvc = msgSvc;
    }

    /**
     * @return  Message service.
     */
    public MessageService messageService() {
        return msgSvc;
    }

    /**
     * @param timeoutSvc Timeout service.
     */
    public void timeoutService(TimeoutService timeoutSvc) {
        this.timeoutSvc = timeoutSvc;
    }

    /**
     * @return Timeout service.
     */
    public TimeoutService timeoutService() {
        return timeoutSvc;
    }

    /** */
    public void queryRegistry(QueryRegistry qryRegistry) {
        this.qryRegistry = qryRegistry;
    }

    /** {@inheritDoc} */
    @Override
    public <Row> void sendBatch(UUID nodeId, UUID qryId, long outboxFragmentId, long exchangeId, int batchId,
                                boolean last, List<Row> rows, RelDataType rowType) throws IgniteCheckedException {
        messageService().send(nodeId, new QueryBatchMessage(qryId, outboxFragmentId, exchangeId, batchId, last, Commons.cast(rows), rowType));

        if (batchId == 0) {
            Query<?> qry = qryRegistry.query(qryId);

            if (qry != null)
                qry.onOutboundExchangeStarted(nodeId, exchangeId);
        }
    }

    /** {@inheritDoc} */
    @Override public void acknowledge(UUID nodeId, UUID qryId, long outboxFragmentId, long exchangeId, int batchId)
        throws IgniteCheckedException {
        messageService().send(nodeId, new QueryBatchAcknowledgeMessage(qryId, exchangeId, outboxFragmentId, batchId));
    }

    /** {@inheritDoc} */
    @Override public void closeQuery(UUID nodeId, UUID qryId) throws IgniteCheckedException {
        messageService().send(nodeId, new QueryCloseMessage(qryId));
    }

    /** {@inheritDoc} */
    @Override public void closeInbox(UUID nodeId, UUID qryId, long outboxFragmentId, long exchangeId) throws IgniteCheckedException {
        messageService().send(nodeId, new InboxCloseMessage(qryId, outboxFragmentId, exchangeId));
    }

    /** {@inheritDoc} */
    @Override public void sendError(UUID nodeId, UUID qryId, long fragmentId, Throwable err) throws IgniteCheckedException {
        messageService().send(nodeId, new ErrorMessage(qryId, fragmentId, err));
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        CalciteQueryProcessor proc =
            Objects.requireNonNull(Commons.lookupComponent(ctx, CalciteQueryProcessor.class));

        taskExecutor(proc.taskExecutor());
        mailboxRegistry(proc.mailboxRegistry());
        messageService(proc.messageService());
        timeoutService(proc.timeoutService());
        queryRegistry(proc.queryRegistry());

        init();
    }

    /** {@inheritDoc} */
    @Override public void init() {
        messageService().register((n, m) -> onMessage(n, (InboxCloseMessage)m), MessageType.QUERY_INBOX_CANCEL_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (QueryBatchAcknowledgeMessage)m), MessageType.QUERY_ACKNOWLEDGE_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (QueryBatchMessage)m), MessageType.QUERY_BATCH_MESSAGE);
        messageService().register((n, m) -> onMessage(n, (QueryCloseMessage)m), MessageType.QUERY_CLOSE_MESSAGE);
    }

    /** {@inheritDoc} */
    @Override public boolean alive(UUID nodeId) {
        return messageService().alive(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void onOutboundExchangeFinished(UUID qryId, long exchangeId) {
        Query<?> qry = qryRegistry.query(qryId);

        if (qry != null)
            qry.onOutboundExchangeFinished(exchangeId);
    }

    /** {@inheritDoc} */
    @Override
    public void onInboundExchangeFinished(UUID nodeId, UUID qryId, long outboxFragmentId) {
        Query<?> qry = qryRegistry.query(qryId);

        if (qry != null)
            qry.onInboundExchangeFinished(nodeId, outboxFragmentId);
    }

    /** {@inheritDoc} */
    @Override
    public UUID localNodeId() {
        return locaNodeId;
    }

    /** */
    protected void onMessage(UUID nodeId, InboxCloseMessage msg) {
        Collection<Inbox<?>> inboxes = mailboxRegistry().inboxes(msg.queryId(), msg.exchangeId());

        if (!F.isEmpty(inboxes)) {
            for (Inbox<?> inbox : inboxes)
                inbox.context().execute(inbox::close, inbox::onError);
        }
        else if (log.isDebugEnabled()) {
            log.debug("Stale inbox cancel message received: [" +
                "nodeId=" + nodeId +
                ", queryId=" + msg.queryId() +
                ", fragmentId=" + msg.outboxFragmentId() +
                ", exchangeId=" + msg.exchangeId() + "]");
        }
    }

    /** */
    protected void onMessage(UUID nodeId, QueryCloseMessage msg) {
        Query<?> qry = qryRegistry.query(msg.queryId());

        if (qry != null)
            qry.cancel();
        else {
            for (Inbox<?> inbox : mailboxRegistry().inboxes(msg.queryId()))
                mailboxRegistry().unregister(inbox);

            if (log.isDebugEnabled()) {
                log.debug("Stale query close message received: [" +
                    "nodeId=" + nodeId +
                    ", queryId=" + msg.queryId() + "]");
            }
        }
    }

    /** */
    protected void onMessage(UUID nodeId, QueryBatchAcknowledgeMessage msg) {
        Outbox<?> outbox = mailboxRegistry().outbox(msg.queryId(), msg.exchangeId(), msg.outboxFragmentId());

        if (outbox != null) {
            try {
                outbox.onAcknowledge(nodeId, msg.batchId());
            } catch (Throwable e) {
                outbox.onError(e);

                throw new IgniteException("Unexpected exception", e);
            }
        } else if (log.isDebugEnabled()) {
            log.debug("Stale acknowledge message received: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "outboxFragmentId=" + msg.outboxFragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + ", " +
                "batchId=" + msg.batchId() + "]");
        }
    }

    /**  */
    protected void onMessage(UUID nodeId, QueryBatchMessage msg) {
        InboxController inboxCtl = mailboxRegistry().inboxController(msg.queryId(), msg.exchangeId());

        if (inboxCtl == null && msg.batchId() == 0) {
            // first message sent before a fragment is built
            inboxCtl = mailboxRegistry().getOrTemporaryController(msg.queryId(), msg.exchangeId());
        }

        if (inboxCtl == null) {
            if (log.isDebugEnabled()) {
                log.debug("Stale batch message received: [" +
                    "nodeId=" + nodeId + ", " +
                    "queryId=" + msg.queryId() + ", " +
                    "outboxFragmentId=" + msg.outboxFragmentId() + ", " +
                    "exchangeId=" + msg.exchangeId() + ", " +
                    "batchId=" + msg.batchId() + "]");
            }
            return;
        }

        if (inboxCtl.ready()) {
            processBatch(inboxCtl, nodeId, msg);
        } else new Thread(() -> {

            // New inbox for query batch message can be registered in the following cases:
            // 1. Race between messages (when first batch arrived to node before query start request). In this case
            // query start request eventually will be delivered and query execution context will be initialized.
            // Inbox will be closed by standard query execution workflow.
            // 2. Stale first message (query already has been closed by some event). In this case query execution
            // workflow already completed and inbox can leak. To prevent leakage, schedule task to check that
            // query context is initialized within reasonable time (assume that race between messages can't be more
            // than INBOX_INITIALIZATION_TIMEOUT milliseconds).

            // We wait in our own thread so we don't block the initialization if it happens on the same thread (or other incoming messages)
            InboxController controller = mailboxRegistry().inboxController(msg.queryId(), msg.exchangeId());
            assert controller != null;

            // Wait for the controller to be ready
            try {
                controller.waitForReady(INBOX_INITIALIZATION_TIMEOUT, TimeUnit.MILLISECONDS);
                controller = mailboxRegistry().inboxController(msg.queryId(), msg.exchangeId());
            } catch (InterruptedException ignored) {
            }

            // Act accordingly
            if (controller != null && controller.ready()) {
                InboxController initializedController = controller;
                taskExecutor().execute(msg.queryId(), msg.executorFragmentId(), () -> processBatch(initializedController, nodeId, msg));
                return;
            }

            // Initialization Failed
            log.warning("Inbox controller failed to initialize in required time: [" +
                "nodeId=" + nodeId + ", " +
                "queryId=" + msg.queryId() + ", " +
                "outboxFragmentId=" + msg.outboxFragmentId() + ", " +
                "exchangeId=" + msg.exchangeId() + ", " +
                "batchId=" + msg.batchId() + "]");

            if (controller != null) {
                controller.error(new QueryCancelledException("Inbox failed to initialize!"));
                controller.close();
            }

        }).start();
    }

    /**
     * Process a QueryBatchMessage
     */
    private void processBatch(InboxController inboxController, UUID nodeId, QueryBatchMessage msg) {
        assert inboxController.ready();
        if (msg.batchId() == 0) {
            Query<?> qry = qryRegistry.query(msg.queryId());

            if (qry != null)
                qry.onInboundExchangeStarted(nodeId, msg.outboxFragmentId());
        }

        inboxController.processBatch(nodeId, msg);
    }

    /**
     * @return Minimal execution context to meet Inbox needs.
     */
    private ExecutionContext<?> baseInboxContext(UUID nodeId, UUID qryId, long fragmentId) {
        return new ExecutionContext<>(
            BaseQueryContext.builder()
                .logger(log)
                .build(),
            taskExecutor(),
            qryId,
            locaNodeId,
            nodeId,
            null,
            new FragmentDescription(
                fragmentId,
                null,
                null,
                null,
                1,
                0),
            null,
            NoOpMemoryTracker.INSTANCE,
            NoOpIoTracker.INSTANCE,
            0,
            ImmutableMap.of());
    }
}
