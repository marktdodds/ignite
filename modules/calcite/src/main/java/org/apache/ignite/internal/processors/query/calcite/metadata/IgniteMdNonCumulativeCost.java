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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;

/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdNonCumulativeCost implements MetadataHandler<BuiltInMetadata.NonCumulativeCost> {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltInMethod.NON_CUMULATIVE_COST.method, new IgniteMdNonCumulativeCost());

    /** */
    @Override public MetadataDef<BuiltInMetadata.NonCumulativeCost> getDef() {
        return BuiltInMetadata.NonCumulativeCost.DEF;
    }

    /** */
    public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
        return rel.computeSelfCost(rel.getCluster().getPlanner(), mq);
    }

    /** Computes the cost of a LogicalJoin. This method gets the base costs and adds the memory size of the
     * left input. The ending result is a plan that has the smaller relation in the left input. It is used
     * in the physical optimization plan that assumes the right relation is larger (for determining join
     * distribution plans). This works in tandem with the JoinCommute rule to explore permutations of join
     * inputs here without expanding the search space to the point Calcite times out. */
    public RelOptCost getNonCumulativeCost(LogicalJoin rel, RelMetadataQuery mq) {
        IgniteCostFactory costFactory = (IgniteCostFactory) rel.getCluster().getPlanner().getCostFactory();
        return rel.computeSelfCost(rel.getCluster().getPlanner(), mq)
            .plus(costFactory.makeCost(0, 0, 0, mq.getRowCount(rel.getLeft()), 0));
    }

    /** */
    public RelOptCost getNonCumulativeCost(RelSubset rel, RelMetadataQuery mq) {
        return mq.getNonCumulativeCost(rel.getBestOrOriginal());
    }

}
