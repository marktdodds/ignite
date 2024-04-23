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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * Relational expression that imposes a particular distribution on its input
 * without otherwise changing its content.
 */
public class IgniteExchange extends Exchange implements IgniteRel {
    /**
     * Creates an Exchange.
     *
     * @param cluster   Cluster this relational expression belongs to
     * @param traitSet  Trait set
     * @param input     Input relational expression
     * @param distribution Distribution specification
     */
    public IgniteExchange(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
        super(cluster, traitSet, input, distribution);
    }

    /** */
    public IgniteExchange(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return (IgniteDistribution)distribution;
    }

    /** {@inheritDoc} */
    @Override public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
        return new IgniteExchange(getCluster(), traitSet, newInput, newDistribution);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(getInput());
        double bytesPerRow = getRowType().getFieldCount() * IgniteCost.AVERAGE_FIELD_SIZE;
        double totalBytes = rowCount * bytesPerRow;
        double latencyPenalty = 0;

        IgniteCostFactory costFactory = (IgniteCostFactory)planner.getCostFactory();

        if (IgniteDistributions.broadcast().equals(distribution)) {
            totalBytes *= IgniteCost.BROADCAST_DISTRIBUTION_PENALTY;
            double exponent = IgniteSystemProperties.getDouble("MD_BROADCAST_LATENCY_PENALTY", 0);
            if (exponent >= 1) latencyPenalty = Math.pow(rowCount, exponent);
        }

        if (IgniteSystemProperties.getBoolean("MD_NO_EXCHANGE_NETWORK_COST", false))
            totalBytes = 0;

        return costFactory.makeCost(rowCount, rowCount * IgniteCost.ROW_PASS_THROUGH_COST, 0, 0, totalBytes, latencyPenalty);
    }

    /** {@inheritDoc} */
    @Override public boolean isEnforcer() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteExchange(cluster, getTraitSet(), sole(inputs), distribution);
    }

}
