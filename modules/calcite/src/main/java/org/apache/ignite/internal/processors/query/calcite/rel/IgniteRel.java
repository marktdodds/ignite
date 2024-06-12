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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

import java.util.List;

/**
 * A superinterface of all Ignite relational nodes.
 */
public interface IgniteRel extends PhysicalNode {
    /**
     * Accepts a visit from a visitor.
     *
     * @param visitor Ignite visitor.
     * @return Visit result.
     */
    <T> T accept(IgniteRelVisitor<T> visitor);

    /**
     * Clones this rel associating it with given cluster.
     *
     * @param cluster Cluster.
     * @return New rel.
     */
    IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs);

    /**
     * @return Node distribution.
     */
    default IgniteDistribution distribution() {
        return TraitUtils.distribution(getTraitSet());
    }

    /**
     * @return Node collations.
     */
    default RelCollation collation() {
        return TraitUtils.collation(getTraitSet());
    }

    /**
     * @return Node rewindability.
     */
    default RewindabilityTrait rewindability() {
        return TraitUtils.rewindability(getTraitSet());
    }

    /**
     * @return Node correlation.
     */
    default CorrelationTrait correlation() {
        return TraitUtils.correlation(getTraitSet());
    }

    /** {@inheritDoc} */
    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        RelTraitSet required) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
        RelTraitSet childTraits, int childId) {
        return null;
    }

    default boolean cacheMatches(IgniteRel other) {
        return false;
    }


    default boolean hasPathWithNoExchange(RelNode rel) {
        if (rel instanceof IgniteExchange) return false;
        if (rel instanceof RelSubset) return hasPathWithNoExchange(((RelSubset) rel).getBestOrOriginal());
        for (RelNode child : rel.getInputs()) {
            if (hasPathWithNoExchange(child)) return true;
        }
        return true;
    }

    /**
     * Returns the distribution factor which is the number of nodes a cache is distributed over.
     */
    default int distributionFactor(RelNode childPath, RelMetadataQuery mq) {
        RelDistribution.Type distrType = distribution().getType();
        switch (distrType) {
            case BROADCAST_DISTRIBUTED:
                return hasPathWithNoExchange(childPath) ? 2 : 1;
            case HASH_DISTRIBUTED:
                return 2;
            default:
                return 1;
        }
    }

}
