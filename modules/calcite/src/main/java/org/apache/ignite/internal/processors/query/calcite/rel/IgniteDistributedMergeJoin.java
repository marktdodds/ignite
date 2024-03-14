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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.externalize.RelInputEx;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Relational expression that combines two relational expressions according to
 * some condition.
 *
 * <p>Each output row has columns from the left and right inputs.
 * The set of output rows is a subset of the cartesian product of the two
 * inputs; precisely which subset depends on the join condition.
 */
public class IgniteDistributedMergeJoin extends IgniteMergeJoin {
    /**  */
    public IgniteDistributedMergeJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType,
            left.getTraitSet().getCollation(), right.getTraitSet().getCollation());
    }

    /**  */
    public IgniteDistributedMergeJoin(RelInput input) {
        super(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getExpression("condition"),
            ImmutableSet.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
            input.getEnum("joinType", JoinRelType.class),
            ((RelInputEx) input).getCollation("leftCollation"),
            ((RelInputEx) input).getCollation("rightCollation")
        );
    }


    /**  */
    private IgniteDistributedMergeJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType,
        RelCollation leftCollation,
        RelCollation rightCollation
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, leftCollation, rightCollation);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) planner.getCostFactory();

        if ("true".equalsIgnoreCase(System.getenv("MD_FORCE_DIST_MJ"))) return costFactory.makeZeroCost();

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

        return computeSelfCost(costFactory, leftCount, rightCount / distributionFactor);
    }

    @Override
    public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        List<Pair<RelTraitSet, List<RelTraitSet>>> res = new ArrayList<>();

        RelTraitSet outTraits = nodeTraits.replace(TraitUtils.distribution(right));
        RelTraitSet leftTraits = left.replace(IgniteDistributions.broadcast());
        RelTraitSet rightTraits = right;
        res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

        // TODO roll mergejoin and this into 1

//        leftTraits = leftTraits.replace(IgniteDistributions.single());
//        rightTraits = rightTraits.replace(IgniteDistributions.single());
//        outTraits = outTraits.replace(IgniteDistributions.single());
//        res.add(Pair.of(outTraits, ImmutableList.of(leftTraits, rightTraits)));

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(
        RelTraitSet desiredNodeTraits,
        List<RelTraitSet> requiredInputTraits
    ) {
//        if (TraitUtils.distribution(desiredNodeTraits).getType() == SINGLETON)
//            return Pair.of(desiredNodeTraits.replace(IgniteDistributions.single()), Commons.transform(requiredInputTraits, t -> t.replace(IgniteDistributions.single())));
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new IgniteDistributedMergeJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType,
            left.getTraitSet().getCollation(), right.getTraitSet().getCollation());
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteDistributedMergeJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
            getVariablesSet(), getJoinType(), leftCollation, rightCollation);
    }
}
