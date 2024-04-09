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

package org.apache.ignite.internal.processors.query.calcite.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;

/**
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
public class IgniteHashJoin extends AbstractIgniteJoin {

    private final RexNode rightFilterCondition;
    private final ImmutableBitSet rightRequiredColumns;

    public IgniteHashJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                          RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        rightFilterCondition = null;
        rightRequiredColumns = null;
    }


    public IgniteHashJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                          RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType,
                          RexNode rightFilterCondition, ImmutableBitSet rightRequiredColumns) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        this.rightFilterCondition = rightFilterCondition;
        this.rightRequiredColumns = rightRequiredColumns;
    }

    /**  */
    public IgniteHashJoin(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getExpression("condition"),
            ImmutableSet.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
            input.getEnum("joinType", JoinRelType.class));
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        if (IgniteSystemProperties.getBoolean("MD_FORCE_DIST_HJ", false)) return costFactory.makeZeroCost();

        double leftCount = mq.getRowCount(getLeft());
        if (Double.isInfinite(leftCount))
            return costFactory.makeInfiniteCost();

        double rightCount = mq.getRowCount(getRight());
        if (Double.isInfinite(rightCount))
            return costFactory.makeInfiniteCost();

        double distributionFactor = 1;

        // Account for distributed join on partition. We assume a roughly even distribution of data
        if (TraitUtils.distribution(getLeft().getTraitSet()).satisfies(IgniteDistributions.broadcast())) {
            RelOptTable table = mq.getTableOrigin(getLeft());
            if (table != null) { // Could be null if we're doing a join of a join
                distributionFactor = table.unwrap(IgniteCacheTable.class).clusterMetrics().getPartitionLayout().size();
            }
        }

        double rightSize = rightCount * getRight().getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;

        return costFactory.makeCost(leftCount + rightCount,
            leftCount * (IgniteCost.HASH_LOOKUP_COST) + rightCount * IgniteCost.ROW_PASS_THROUGH_COST / distributionFactor, // We do {leftCount} comparisons on a {rightCount} size hash table
            0,
            rightSize / distributionFactor,
            0);
    }

    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        List<Pair<RelTraitSet, List<RelTraitSet>>> res = new ArrayList<>(super.deriveDistribution(nodeTraits, inputTraits));

        // Broadcast the left input
        RelTraitSet outTraits = nodeTraits.replace(TraitUtils.distribution(right));
        RelTraitSet leftTraits = left.replace(IgniteDistributions.broadcast());
        RelTraitSet rightTraits = right;
        res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

        // Single site join
//        outTraits = outTraits.replace(IgniteDistributions.single());
//        leftTraits = leftTraits.replace(IgniteDistributions.single());
//        rightTraits = rightTraits.replace(IgniteDistributions.single());
//        res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(
        RelTraitSet desiredNodeTraits,
        List<RelTraitSet> requiredInputTraits
    ) {
        if (TraitUtils.distribution(desiredNodeTraits).getType() == SINGLETON)
            return Pair.of(desiredNodeTraits.replace(IgniteDistributions.single()), Commons.transform(requiredInputTraits, t -> t.replace(IgniteDistributions.single())));

        return null;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("rightFilter", getRightFilterCondition())
            .item("rightRequiredColumns", getRightRequiredColumns())
            .item("dist", distribution())
            ;
    }

    /** {@inheritDoc} */
    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new IgniteHashJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType, getRightFilterCondition(), getRightRequiredColumns());
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteHashJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
            getVariablesSet(), getJoinType(), getRightFilterCondition(), getRightRequiredColumns());
    }

    /**
     * Clones the Rel with a condition and/or column requirement on the right input. Used when a TableScan predicates
     * are pushing into the HashJoin so a complete hashtable can be cached.
     */
    public IgniteHashJoin clone(RexNode rightFilterCondition, ImmutableBitSet rightRequiredColumns, IgniteRel rightInput) {
        return new IgniteHashJoin(getCluster(), getTraitSet(), getLeft(), rightInput, getCondition(), getVariablesSet(), getJoinType(), rightFilterCondition, rightRequiredColumns);
    }

    @Override
    public boolean cacheMatches(IgniteRel other) {
        if (!(other instanceof IgniteHashJoin)) return false;
        IgniteHashJoin otherJoin = (IgniteHashJoin) other;

        if (!otherJoin.getCondition().isA(SqlKind.EQUALS)) return false;
        if (!otherJoin.getRight().getRowType().equals(getRight().getRowType())) return false;

        // We build the HashTable on the right column so that is the part we care about matching
        Set<Integer> rightConditions = new HashJoinConditionExtractor(getLeft().getRowType().getFieldCount()).go(getCondition());
        Set<Integer> otherRightConditions = new HashJoinConditionExtractor(otherJoin.getLeft().getRowType().getFieldCount()).go(otherJoin.getCondition());

        if (rightConditions.isEmpty() || otherRightConditions.isEmpty()) return false;

        return rightConditions.equals(otherRightConditions);
    }

    /**
     * Creates a HashJoin
     */
    public RexNode getRightFilterCondition() {
        return rightFilterCondition;
    }

    public ImmutableBitSet getRightRequiredColumns() {
        return rightRequiredColumns;
    }

    private static class HashJoinConditionExtractor implements RexVisitor<Set<Integer>> {

        private final int rightRowThreshold;

        HashJoinConditionExtractor(int rightRowThreshold) {
            this.rightRowThreshold = rightRowThreshold;
        }

        public Set<Integer> go(RexNode r) {
            return r.accept(this);
        }

        @Override
        public Set<Integer> visitInputRef(RexInputRef rexInputRef) {
            return rexInputRef.getIndex() >= rightRowThreshold ? new HashSet<>(Collections.singletonList(rexInputRef.getIndex())) : null;
        }

        @Override
        public Set<Integer> visitLocalRef(RexLocalRef rexLocalRef) {
            return null;
        }

        @Override
        public Set<Integer> visitLiteral(RexLiteral rexLiteral) {
            return null;
        }

        @Override
        public Set<Integer> visitCall(RexCall rexCall) {
            Set<Integer> result = new HashSet<>();
            for (RexNode r : rexCall.getOperands()) {
                Optional.ofNullable(r.accept(this)).ifPresent(result::addAll);
            }
            return result;
        }

        @Override
        public Set<Integer> visitOver(RexOver rexOver) {
            return null;
        }

        @Override
        public Set<Integer> visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
            return null;
        }

        @Override
        public Set<Integer> visitDynamicParam(RexDynamicParam rexDynamicParam) {
            return null;
        }

        @Override
        public Set<Integer> visitRangeRef(RexRangeRef rexRangeRef) {
            return null;
        }

        @Override
        public Set<Integer> visitFieldAccess(RexFieldAccess rexFieldAccess) {
            return null;
        }

        @Override
        public Set<Integer> visitSubQuery(RexSubQuery rexSubQuery) {
            return null;
        }

        @Override
        public Set<Integer> visitTableInputRef(RexTableInputRef rexTableInputRef) {
            return null;
        }

        @Override
        public Set<Integer> visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
            return null;
        }
    }

}
