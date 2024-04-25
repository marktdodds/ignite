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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexHashKey;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexHasher;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.ObjectSizeCalculator;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**  */
public abstract class HashJoinNode<Row> extends MemoryTrackingNode<Row> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    protected final List<Row> EMPTY_LIST = (List<Row>) Commons.EMPTY_LIST;
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
    protected Predicate<Row> rightPreJoinPredicate;

    /**  */
    private ImmutableBitSet rightPreJoinColumnFilter;

    /**  */
    protected RelDataType rightRowType;

    /**  */
    protected Map<RexHashKey, List<Row>> rightMaterialized = new HashMap<>(IN_BUFFER_SIZE);

    /**  */
    private final ObjectSizeCalculator<Object[]> hashMapKeySizeCalculator = new ObjectSizeCalculator<>();

    /**  */
    private final ObjectSizeCalculator<Row> hashMapEntrySizeCalculator = new ObjectSizeCalculator<>();

    /** Tracks the memory allocation of the hash map */
    private long hashMapMemoryUsed;

    /** Right row factory. */
    protected RowHandler.RowFactory<Row> rightRowFactory;

    /**  */
    protected final Deque<Row> leftInBuf = new ArrayDeque<>(IN_BUFFER_SIZE);
    protected final Deque<Row> outboxBuf = new ArrayDeque<>(IO_BATCH_SIZE);

    /**  */
    protected boolean inLoop;
    private Runnable onComplete;

    /**
     * @param ctx          Execution context.
     * @param cond         Join expression.
     * @param rightRowType Right row type
     */
    private HashJoinNode(ExecutionContext<Row> ctx, RelDataType rowType, RexNode cond, RelDataType leftRowType, RelDataType rightRowType) {
        super(ctx, rowType);

        this.cond = cond;
        this.leftRowType = leftRowType;
        this.rightRowType = rightRowType;

        handler = ctx.rowHandler();
        rightRowFactory = handler.factory(ctx.getTypeFactory(), rightRowType);
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

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;

        leftInBuf.clear();
        outboxBuf.clear();

        if (waitingRight != NOT_WAITING) {
            waitingRight = 0;
            rightMaterialized.clear();
        }
        // If we are rewinding we should be able to keep the right HashMap the same?
        // Todo this could be a spot for content validation problems...
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
        hashMapMemoryUsed += hashKey.getByteSize(hashMapKeySizeCalculator) + hashMapEntrySizeCalculator.sizeOf(row);

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

    }

    protected void emptyOutbox() throws Exception {
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
    private void doJoin() throws Exception {
        checkState();

        join();
    }

    /**
     * Called when the join() has completed. Runs cleanup and an optional callback (used
     * for result caching)
     */
    protected void joinCompleted() throws Exception {
        requested = 0;
        downstream().end();
        if (onComplete != null) new Thread(onComplete).start();
    }

    /**  */
    @NotNull
    public static <Row> HashJoinNode<Row> create(ExecutionContext<Row> ctx, RelDataType outputRowType,
                                                 RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType,
                                                 RexNode cond) {
        switch (joinType) {
            case INNER:
                return new InnerJoin<>(ctx, outputRowType, cond, leftRowType, rightRowType);

            case LEFT:
                return new LeftJoin<>(ctx, outputRowType, cond, leftRowType, rightRowType);

            case RIGHT:
                return new RightJoin<>(ctx, outputRowType, cond, leftRowType, rightRowType);

            case FULL:
                return new FullOuterJoin<>(ctx, outputRowType, cond, leftRowType, rightRowType);

            case SEMI:
                return new SemiJoin<>(ctx, outputRowType, cond, leftRowType, rightRowType);

            case ANTI:
                return new AntiJoin<>(ctx, outputRowType, cond, leftRowType, rightRowType);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    /**
     * Sets a column filter on the right input to be applied during the join
     * Pushed up from a table scan so the hashtable built here is cacheable
     */
    public void setRightColumnFilter(ImmutableBitSet rightRequiredColumns) {
        rightPreJoinColumnFilter = rightRequiredColumns;

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(context().getTypeFactory());
        List<RelDataTypeField> fieldList = rightRowType.getFieldList();
        for (int i = rightRequiredColumns.nextSetBit(0); i != -1; i = rightRequiredColumns.nextSetBit(i + 1))
            b.add(fieldList.get(i));

        rightRowType = b.build();
        rightRowFactory = handler.factory(context().getTypeFactory(), rightRowType);

    }

    /**
     * Sets a predicate to be applied on the right input during the join
     */
    public void setRightPreJoinPredicate(Predicate<Row> pred) {
        rightPreJoinPredicate = pred;
    }

    /**
     * Injects a previously used and cached HashJoinNode into this execution node.
     */
    public void injectFromCache(HashJoinNode<Row> cached) {
        rightMaterialized = cached.rightMaterialized;
    }

    /**
     * Applies a predicate and/or column filter to an input row
     */
    protected @Nullable Row applyRightPredicateAndColumnFilter(Row row) {
        Row realRow = row;
        if (rightPreJoinColumnFilter != null) {
            realRow = rightRowFactory.create();
            int colIdx = 0;
            for (int i = rightPreJoinColumnFilter.nextSetBit(0); i != -1; i = rightPreJoinColumnFilter.nextSetBit(i + 1))
                handler.set(colIdx++, realRow, handler.get(i, row));
        }
        if (rightPreJoinPredicate != null && !rightPreJoinPredicate.test(realRow)) return null;
        return realRow;
    }

    /**
     * Sets a callback for when the join is completed
     */
    public void setOnComplete(Runnable runner) {
        assert onComplete == null;
        onComplete = runner;
    }

    /**
     * Returns the est. memory size of this node if it were cached.
     */
    public long getCacheMemorySize() {
        return hashMapMemoryUsed;
    }

    /**  */
    private static class InnerJoin<Row> extends HashJoinNode<Row> {

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public InnerJoin(ExecutionContext<Row> ctx, RelDataType rowType, RexNode cond, RelDataType leftRowType, RelDataType rightRowType) {
            super(ctx, rowType, cond, leftRowType, rightRowType);
        }

        /**  */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                checkState();
                inLoop = true;
                try {

                    // Empty the outbox buffer first
                    emptyOutbox();

                    // Try and process new rows
                    while (requested > 0 && !leftInBuf.isEmpty()) {
                        Row left = leftInBuf.remove();

                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.getOrDefault(hashId, EMPTY_LIST);

                        for (Row right : bucket) {
                            Row filtered = applyRightPredicateAndColumnFilter(right);
                            if (filtered == null) continue;
                            pushDownstream(handler.concat(left, filtered));
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
                joinCompleted();
            }
        }
    }

    /**  */
    private static class LeftJoin<Row> extends HashJoinNode<Row> {

        /**
         * @param ctx          Execution context.
         * @param cond         Join expression.
         * @param rightRowType Right row type definition
         */
        public LeftJoin(
            ExecutionContext<Row> ctx,
            RelDataType rowType,
            RexNode cond,
            RelDataType leftRowType,
            RelDataType rightRowType) {
            super(ctx, rowType, cond, leftRowType, rightRowType);
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
                checkState();
                inLoop = true;
                try {

                    emptyOutbox();

                    while (requested > 0 && !leftInBuf.isEmpty()) {
                        Row left = leftInBuf.remove();

                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.get(hashId);
                        if (bucket == null) {
                            pushDownstream(handler.concat(left, rightRowFactory.create()));
                        } else for (Row right : bucket) {
                            Row filtered = applyRightPredicateAndColumnFilter(right);
                            if (filtered == null) continue;
                            pushDownstream(handler.concat(left, filtered));
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
                joinCompleted();
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
            RelDataType leftRowType,
            RelDataType rightRowType) {
            super(ctx, rowType, cond, leftRowType, rightRowType);

            leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);
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
                checkState();
                inLoop = true;

                // Setup the set for non-matched elements
                if (rightNotMatched == null) {
                    rightNotMatched = new HashSet<>();
                    rightNotMatched.addAll(rightMaterialized.keySet());
                }

                try {
                    emptyOutbox();

                    while (requested > 0 && !leftInBuf.isEmpty()) {

                        Row left = leftInBuf.remove();

                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.getOrDefault(hashId, EMPTY_LIST);
                        boolean sent = false;
                        for (Row right : bucket) {
                            Row filtered = applyRightPredicateAndColumnFilter(right);
                            if (filtered == null) continue;
                            pushDownstream(handler.concat(left, filtered));
                            sent = true;
                        }

                        if (sent) rightNotMatched.remove(hashId);
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
                                Row filtered = applyRightPredicateAndColumnFilter(right);
                                if (filtered == null) continue;
                                pushDownstream(handler.concat(leftRowFactory.create(), filtered));
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
                joinCompleted();
            }
        }
    }

    /**  */
    private static class FullOuterJoin<Row> extends HashJoinNode<Row> {
        /** Left row factory. */
        private final RowHandler.RowFactory<Row> leftRowFactory;
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
            RelDataType leftRowType,
            RelDataType rightRowType) {
            super(ctx, rowType, cond, leftRowType, rightRowType);

            leftRowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), leftRowType);
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

                checkState();
                inLoop = true;
                try {

                    emptyOutbox();
                    while (requested > 0 && !leftInBuf.isEmpty()) {

                        Row left = leftInBuf.remove();

                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.getOrDefault(hashId, new ArrayList<>());
                        boolean sent = false;
                        for (Row right : bucket) {
                            // Inner
                            Row filtered = applyRightPredicateAndColumnFilter(right);
                            if (filtered == null) continue;
                            pushDownstream(handler.concat(left, filtered));
                            sent = true;
                        }

                        if (!sent) // Left outer
                            pushDownstream(handler.concat(left, rightRowFactory.create()));
                        else
                            rightNotMatched.remove(hashId);

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
                                Row filtered = applyRightPredicateAndColumnFilter(right);
                                if (filtered == null) continue;
                                pushDownstream(handler.concat(leftRowFactory.create(), filtered)); // Right outer
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
                joinCompleted();
            }
        }
    }

    /**  */
    private static class SemiJoin<Row> extends HashJoinNode<Row> {

        /**
         * @param ctx  Execution context.
         * @param cond Join expression.
         */
        public SemiJoin(ExecutionContext<Row> ctx, RelDataType rowType, RexNode cond, RelDataType leftRowType, RelDataType rightRowType) {
            super(ctx, rowType, cond, leftRowType, rightRowType);
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                checkState();

                emptyOutbox();

                while (requested > 0 && !leftInBuf.isEmpty()) {


                    Row left = leftInBuf.remove();
                    RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                    List<Row> bucket = rightMaterialized.getOrDefault(hashId, EMPTY_LIST);

                    // Because we potentially moved predicates up we have to ensure we have at
                    // least 1 valid right row or we'll get incorrect results
                    for (Row right : bucket) {
                        Row filtered = applyRightPredicateAndColumnFilter(right);
                        if (filtered == null) continue;
                        pushDownstream(left);
                        break;
                    }

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
        public AntiJoin(ExecutionContext<Row> ctx, RelDataType rowType, RexNode cond, RelDataType leftRowType, RelDataType rightRowType) {
            super(ctx, rowType, cond, leftRowType, rightRowType);
        }


        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                checkState();
                inLoop = true;
                try {

                    emptyOutbox();

                    while (requested > 0 && !leftInBuf.isEmpty()) {

                        Row left = leftInBuf.remove();
                        RexHashKey hashId = new RexHasher<>(handler, 0, left).go(cond);
                        List<Row> bucket = rightMaterialized.getOrDefault(hashId, EMPTY_LIST);
                        // Because we potentially moved predicates up we have to ensure we have at
                        // least 1 valid right row or we'll get incorrect results
                        for (Row r : bucket) {
                            if (applyRightPredicateAndColumnFilter(r) == null) continue;
                            pushDownstream(left);
                            break;
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
                joinCompleted();
            }
        }
    }
}
