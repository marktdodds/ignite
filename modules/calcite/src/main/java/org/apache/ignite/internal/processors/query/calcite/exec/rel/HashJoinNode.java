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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexHashKey;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexHasher;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.util.InternalDebug;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**  */
public abstract class HashJoinNode<Row> extends MemoryTrackingNode<Row> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    protected static final int NOT_WAITING = -1;

    /**  */
    protected final RexNode cond;

    /**  */
    protected final RowHandler<Row> handler;

    /**  */
    protected int requested;

    /**  */
    protected int waitingLeft;

    /**  */
    protected int waitingRight;

    /**  */
    protected RelDataType leftRowType;

    /**  */
    protected final Map<RexHashKey, List<Row>> rightMaterialized = new HashMap<>(IN_BUFFER_SIZE);

    /**  */
    protected final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);
    protected final Deque<Row> outboxBuf = new ArrayDeque<>(IO_BATCH_SIZE);

    /**  */
    protected boolean inLoop;

    protected InternalDebug debug = InternalDebug.once("HJ");
    protected InternalDebug timer = InternalDebug.once("HJTimer");


    /**
     * @param ctx  Execution context.
     * @param cond Join expression.
     */
    private HashJoinNode(ExecutionContext<Row> ctx, RelDataType rowType, RexNode cond, RelDataType leftRowType) {
        super(ctx, rowType);

        this.cond = cond;
        handler = ctx.rowHandler();
        this.leftRowType = leftRowType;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::doJoin, this::onError);
    }

    /**  */
    private void doJoin() throws Exception {
        checkState();

        join();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        outboxBuf.clear();
        rightMaterialized.clear();
        leftInBuf.clear();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<Row> requestDownstream(int idx) {
        if (idx == 0)
            return new Downstream<Row>() {
                /** {@inheritDoc} */
                @Override
                public void push(Row row) throws Exception {
                    pushLeft(row);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endLeft();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    HashJoinNode.this.onError(e);
                }
            };
        else if (idx == 1)
            return new Downstream<Row>() {
                /** {@inheritDoc} */
                @Override
                public void push(Row row) throws Exception {
                    pushRight(row);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endRight();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    HashJoinNode.this.onError(e);
                }
            };

        throw new IndexOutOfBoundsException();
    }

    /**  */
    private void pushLeft(Row row) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft--;

        leftInBuf.add(row);

        join();
    }

    /**  */
    private void pushRight(Row row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        RexHashKey hashKey = new RexHasher<>(handler, leftRowType.getFieldCount(), row).go(cond);
        if (!rightMaterialized.containsKey(hashKey)) rightMaterialized.put(hashKey, new ArrayList<>());
        rightMaterialized.get(hashKey).add(row);

        nodeMemoryTracker.onRowAdded(row);

        if (waitingRight == 0)
            rightSource().request(waitingRight = IN_BUFFER_SIZE);
    }

    /**  */
    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        checkState();

        waitingLeft = NOT_WAITING;

        join();
    }

    /**  */
    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight = NOT_WAITING;

        join();
    }

    /** Push downstream or into the outbox if full */
    protected void pushDownstream(Row r) throws Exception {
        if (requested > 0) {
            requested--;
            downstream().push(r);
        } else
            outboxBuf.push(r);

        debug.counterInc();
    }

    protected void emptyOutbox() throws Exception {
        checkState();
        while (requested > 0 && !outboxBuf.isEmpty()) {
            requested--;
            downstream().push(outboxBuf.remove());
        }
    }

    /**  */
    protected Node<Row> leftSource() {
        return sources().get(0);
    }

    /**  */
    protected Node<Row> rightSource() {
        return sources().get(1);
    }

    /**  */
    protected abstract void join() throws Exception;

    /**  */
    @NotNull
    public static <Row> HashJoinNode<Row> create(ExecutionContext<Row> ctx, RelDataType outputRowType,
                                                 RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType,
                                                 RexNode cond,
                                                 RelDataType resultRowType) {
        switch (joinType) {
            case INNER:
                return new InnerJoin<>(ctx, outputRowType, cond, leftRowType);

            case LEFT: {
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new LeftJoin<>(ctx, outputRowType, cond, rightRowFactory, leftRowType);
            }

            case RIGHT: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);

                return new RightJoin<>(ctx, outputRowType, cond, leftRowFactory, leftRowType);
            }

            case FULL: {
                RowHandler.RowFactory<Row> leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);
                RowHandler.RowFactory<Row> rightRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rightRowType);

                return new FullOuterJoin<>(ctx, outputRowType, cond, leftRowFactory, rightRowFactory, leftRowType);
            }

            case SEMI:
                return new SemiJoin<>(ctx, outputRowType, cond, leftRowType);

            case ANTI:
                return new AntiJoin<>(ctx, outputRowType, cond, leftRowType);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    /**  */
    private static class InnerJoin<Row> extends HashJoinNode<Row> {

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public InnerJoin(ExecutionContext<Row> ctx, RelDataType rowType, RexNode cond, RelDataType leftRowType) {
            super(ctx, rowType, cond, leftRowType);
        }

        /**  */
        @Override
        protected void join() throws Exception {
            timer.counterSub(System.currentTimeMillis());
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {

                    // Empty the outbox buffer first
                    emptyOutbox();

                    // Try and process new rows
                    while (requested > 0 && !leftInBuf.isEmpty()) {
                        checkState();
                        Row left = leftInBuf.remove();

                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.get(hashId);

                        if (bucket == null) continue;
                        for (Row right : bucket) {
                            Row out = handler.concat(left, right);
                            pushDownstream(out);
                        }

                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0)
                rightSource().request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                leftSource().request(waitingLeft = IN_BUFFER_SIZE);

            timer.counterAdd(System.currentTimeMillis());
            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
                debug.logCounter("HJ PR", System.out);
                timer.logCounter("HJ PT", System.out);
            }
        }
    }

    /**  */
    private static class LeftJoin<Row> extends HashJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public LeftJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            RexNode cond,
            RowHandler.RowFactory<Row> rightRowFactory,
            RelDataType resultRowType
        ) {
            super(ctx, rowType, cond, resultRowType);

            this.rightRowFactory = rightRowFactory;
        }

        /**  */
        @Override
        protected void rewindInternal() {
            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                // Do the join
                inLoop = true;
                try {

                    emptyOutbox();

                    while (requested > 0 && !leftInBuf.isEmpty()) {
                        Row left = leftInBuf.remove();

                        checkState();

                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.get(hashId);
                        if (bucket == null) {
                            pushDownstream(handler.concat(left, rightRowFactory.create()));
                        } else for (Row right : bucket) {
                            Row row = handler.concat(left, right);
                            pushDownstream(row);
                        }

                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0)
                rightSource().request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                leftSource().request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /**  */
    private static class RightJoin<Row> extends HashJoinNode<Row> {
        /** Right row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /**  */
        private Set<RexHashKey> rightNotMatched;

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public RightJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            RexNode cond,
            RowHandler.RowFactory<Row> leftRowFactory,
            RelDataType resultRowType
        ) {
            super(ctx, rowType, cond, resultRowType);

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            rightNotMatched.clear();
            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;

                // Setup the set for non-matched elements
                if (rightNotMatched == null) {
                    rightNotMatched = new HashSet<>();
                    rightNotMatched.addAll(rightMaterialized.keySet());
                }

                try {
                    emptyOutbox();

                    while (requested > 0 && !leftInBuf.isEmpty()) {

                        checkState();
                        Row left = leftInBuf.remove();

                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.get(hashId);
                        if (bucket == null) continue;

                        rightNotMatched.remove(hashId);
                        for (Row right : bucket) {
                            pushDownstream(handler.concat(left, right));
                        }
                    }
                } finally {
                    inLoop = false;
                }

                // No more left items so push all the right items that werent matched.
                if (waitingLeft == NOT_WAITING && requested > 0 && leftInBuf.isEmpty()) {
                    inLoop = true;

                    try {
                        emptyOutbox();
                        Iterator<RexHashKey> it = rightNotMatched.iterator();
                        while (requested > 0 && it.hasNext()) {
                            checkState();
                            RexHashKey key = it.next();
                            it.remove();

                            for (Row right : rightMaterialized.get(key)) {
                                Row row = handler.concat(leftRowFactory.create(), right);
                                pushDownstream(row);
                            }
                        }
                    } finally {
                        inLoop = false;
                    }
                }
            }

            if (waitingRight == 0)
                rightSource().request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                leftSource().request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty() && rightNotMatched.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /**  */
    private static class FullOuterJoin<Row> extends HashJoinNode<Row> {
        /** Left row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<Row> rightRowFactory;

        /**  */
        private Set<RexHashKey> rightNotMatched;


        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public FullOuterJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            RexNode cond,
            RowHandler.RowFactory<Row> leftRowFactory,
            RowHandler.RowFactory<Row> rightRowFactory,
            RelDataType resultRowType
        ) {
            super(ctx, rowType, cond, resultRowType);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            rightNotMatched.clear();
            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatched == null) {
                    rightNotMatched = new HashSet<>();
                    rightNotMatched.addAll(rightMaterialized.keySet());
                }

                inLoop = true;
                try {

                    emptyOutbox();
                    while (requested > 0 && !leftInBuf.isEmpty()) {

                        checkState();
                        Row left = leftInBuf.remove();

                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.get(hashId);
                        if (bucket == null) {
                            // Left outer
                            pushDownstream(handler.concat(left, rightRowFactory.create()));
                            continue;
                        }

                        rightNotMatched.remove(hashId);
                        for (Row right : bucket) {
                            // Inner
                            pushDownstream(handler.concat(left, right));
                        }

                    }
                } finally {
                    inLoop = false;
                }

                // No more left items so push all the right items that werent matched.
                // noinspection DuplicatedCode
                if (waitingLeft == NOT_WAITING && requested > 0 && leftInBuf.isEmpty()) {
                    inLoop = true;

                    try {
                        emptyOutbox();
                        Iterator<RexHashKey> it = rightNotMatched.iterator();
                        while (requested > 0 && it.hasNext()) {
                            checkState();
                            RexHashKey key = it.next();
                            it.remove();

                            for (Row right : rightMaterialized.get(key)) {
                                Row row = handler.concat(leftRowFactory.create(), right);
                                pushDownstream(row);
                            }
                        }
                    } finally {
                        inLoop = false;
                    }
                }

            }

            if (waitingRight == 0)
                rightSource().request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                leftSource().request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty() && rightNotMatched.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    /**  */
    private static class SemiJoin<Row> extends HashJoinNode<Row> {

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public SemiJoin(ExecutionContext<Row> ctx, RelDataType rowType, RexNode cond, RelDataType resultRowType) {
            super(ctx, rowType, cond, resultRowType);
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && !leftInBuf.isEmpty()) {

                    checkState();

                    Row left = leftInBuf.remove();
                    RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                    if (rightMaterialized.containsKey(hashId)) pushDownstream(left);

                }
            }

            if (waitingRight == 0)
                rightSource().request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                leftSource().request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty()) {
                downstream().end();
                requested = 0;
            }
        }
    }

    /**  */
    private static class AntiJoin<Row> extends HashJoinNode<Row> {
        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public AntiJoin(ExecutionContext<Row> ctx, RelDataType rowType, RexNode cond, RelDataType resultRowType) {
            super(ctx, rowType, cond, resultRowType);
        }


        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && !leftInBuf.isEmpty()) {

                        checkState();
                        Row left = leftInBuf.remove();
                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        if (!rightMaterialized.containsKey(hashId)) pushDownstream(left);
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0)
                rightSource().request(waitingRight = IN_BUFFER_SIZE);

            if (waitingLeft == 0 && leftInBuf.isEmpty())
                leftSource().request(waitingLeft = IN_BUFFER_SIZE);

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }
}
