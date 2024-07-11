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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
@Value.Enclosing
public class JoinExpressionSimplifierRule extends RelRule<JoinExpressionSimplifierRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /**  */
    public JoinExpressionSimplifierRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!IgniteSystemProperties.getBoolean("MD_JOIN_SIMPLIFIER_RULE", false)) return false;
        final LogicalJoin join = call.rel(0);
        return join.getCondition().isA(SqlKind.OR);
    }

    /** {@inheritDoc} */
    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        assert join.getCondition().isA(SqlKind.OR) && join.getCondition() instanceof RexCall;

        final RelBuilder relBuilder = call.builder();
        final RexCall joinCondition = (RexCall) join.getCondition();

        // Get any common operands
        List<RexNode> commonOperands = extractCommonOperandsInOR(joinCondition);

        if (commonOperands.isEmpty()) return;

        // Push the common operands into either the root of the join or its children
        List<RexNode> joinFilters = new ArrayList<>();
        List<RexNode> leftFilters = new ArrayList<>();
        List<RexNode> rightFilters = new ArrayList<>();

        RexCall newJoinCondition = filterOperands(joinCondition, commonOperands);

        if (!RelOptUtil.classifyFilters(
            join,
            commonOperands,
            true,
            true,
            true,
            joinFilters,
            leftFilters,
            rightFilters)) {
            // Its possible we extracted common operands that are not literals, and we disable pushInto so
            // this could fail and return nothing
            return;
        }

        // Could not push all the common operands. This shouldn't really happen but is a safety check just in case.
        if (!commonOperands.isEmpty()) return;

        // Create the new filters
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelNode newJoin = join;

        if (!rightFilters.isEmpty())
            right = relBuilder.push(right).filter(rightFilters).build();

        if (!leftFilters.isEmpty())
            left = relBuilder.push(left).filter(leftFilters).build();

        if (!joinFilters.isEmpty())
            newJoin = relBuilder.push(left)
                .push(right)
                .join(join.getJoinType(), joinFilters)
                .filter(newJoinCondition)
                .build();
        else
            newJoin = relBuilder.push(left).push(right).join(join.getJoinType(), newJoinCondition).build();
//            newJoin.copy(newJoin.getTraitSet(), ImmutableList.of(left, right));

        call.transformTo(newJoin);

    }

    private RexCall filterOperands(RexCall root, List<RexNode> conds) {
        assert root.getKind() == SqlKind.OR;
        List<RexNode> newOperands = new ArrayList<>();

        if (conds.isEmpty()) return root;

        for (RexCall call : Commons.<RexCall>cast(root.getOperands())) {
            List<RexNode> filteredOps = new ArrayList<>();
            if (call.isA(SqlKind.AND)) {
                for (RexNode child : call.getOperands()) {
                    if (!conds.contains(child)) filteredOps.add(child);
                }
            } else {
                if (!conds.contains(call)) filteredOps.add(call);
            }
            newOperands.add(call.clone(call.getType(), filteredOps));
        }
        return root.clone(root.getType(), newOperands);
    }

    private List<RexNode> extractCommonOperandsInOR(RexCall call) {
        assert call.getKind() == SqlKind.OR;

        Map<RexNode, Boolean> possibles = new HashMap<>();
        if (call.getOperands().size() == 0) return new ArrayList<>();

        // Populate the possibles from the first operator
        final RexCall first = (RexCall) call.getOperands().get(0);

        if (first.isA(SqlKind.AND)) {

            // If its an AND any of the AND operands could be extracted. This is the most common case
            for (RexNode child : first.getOperands()) {
                possibles.put(child, false);
            }
        } else {
            possibles.put(first, false);
        }

        // Check the remaining operands for matches.
        for (RexCall rexNode : Commons.<RexCall>cast(call.getOperands())) {
            switch (rexNode.getKind()) {
                case OR:
                    throw new UnsupportedOperationException("Unexpected RexNode OR in OR");
                case AND:
                    for (RexNode child : rexNode.getOperands()) {
                        possibles.computeIfPresent(child, (x, y) -> true);
                    }
                    break;
                default:
                    possibles.computeIfPresent(rexNode, (x, y) -> true);
            }

            // Filter out any operands not found, and reset the found ones for the next search
            possibles = possibles.entrySet().stream()
                .filter(Map.Entry::getValue)
                .collect(Collectors.toMap(Map.Entry::getKey, rexNodeBooleanEntry -> false));
        }

        return new ArrayList<>(possibles.keySet());
    }


    /**
     *
     */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /**  */
        JoinExpressionSimplifierRule.Config DEFAULT = ImmutableJoinExpressionSimplifierRule.Config.of()
            .withDescription("JoinExpressionSimplifierRule")
            .withOperandFor(LogicalJoin.class);

        /** Defines an operand tree for the given classes. */
        default JoinExpressionSimplifierRule.Config withOperandFor(Class<? extends LogicalJoin> rel) {
            return withOperandSupplier(
                o0 -> o0.operand(rel).anyInputs()
            )
                .as(JoinExpressionSimplifierRule.Config.class);
        }

        /** {@inheritDoc} */
        @Override
        default JoinExpressionSimplifierRule toRule() {
            return new JoinExpressionSimplifierRule(this);
        }
    }
}
