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

package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteRexVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;

/**
 * Ignite Join converter.
 */
public class HashJoinConverterRule extends AbstractIgniteConverterRule<LogicalJoin> {
    /**  */
    public static final RelOptRule INSTANCE = new HashJoinConverterRule();

    /**
     * Creates a converter.
     */
    public HashJoinConverterRule() {
        super(LogicalJoin.class, "HashJoinConverter");
    }


    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin oper = call.rel(0);
        return oper.getCondition().accept(new ConditionValidator());
    }

    /** {@inheritDoc} */
    @Override
    protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalJoin rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet leftInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet rightInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);

        RelNode left = convert(rel.getLeft(), leftInTraits);
        RelNode right = convert(rel.getRight(), rightInTraits);

        return new IgniteHashJoin(cluster, outTraits, left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());
    }

    private static class ConditionValidator extends IgniteRexVisitor<Boolean> {

        @Override
        public Boolean visitInputRef(RexInputRef inputRef) {
            return true;
        }

        @Override
        public Boolean visitLocalRef(RexLocalRef localRef) {
            return true;
        }

        @Override
        public Boolean visitCall(RexCall call) {
            switch (call.getKind()) {
                case AND:
                    if (!IgniteSystemProperties.getBoolean("MD_ALLOW_AND_HJ", false)) return false;
                case EQUALS:
                    assert call.getOperands().size() == 2;
                    return call.getOperands().get(0).accept(this) && call.getOperands().get(1).accept(this);
                default:
                    return false;
            }
        }

        /* ======================
         * The rest are TBD
         * ======================
         */

        @Override
        public Boolean defaultValue() {
            return false;
        }
    }

}
