package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
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
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.cache.CacheableIgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;

@Value.Enclosing
public class MakeHashJoinCachableRule extends RelRule<MakeHashJoinCachableRule.Config> implements TransformationRule {

    protected MakeHashJoinCachableRule(Config c) {
        super(c);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        assert call.rel(0) instanceof IgniteHashJoin;
        assert call.rel(2) instanceof IgniteTableScan;

        IgniteHashJoin join = call.rel(0);
        IgniteTableScan scan = call.rel(2);

        // Generate a replacement map so we can modify predicates since we are pushing the column filters up into the join
        ImmutableBitSet scanColumnFilter = scan.requiredColumns();
        HashMap<Integer, Integer> replacements = new HashMap<>();

        if (scanColumnFilter != null) {
            int leftOffset = join.getLeft().getRowType().getFieldCount();
            int index = 0;
            int columnIndex = scanColumnFilter.nextSetBit(0);

            while (columnIndex >= 0) {
                replacements.put(leftOffset + index++, leftOffset + columnIndex);
                columnIndex = scanColumnFilter.nextSetBit(columnIndex + 1);
            }
        }

        // Create the new nodes and predicates
        IgniteTableScan newScan = scan.clone((RexNode) null, null);
        RexNode newJoinCondition = new RexInputReplacer(TypeUtils.combinedRowType(Commons.typeFactory(), join.getLeft().getRowType(), newScan.getRowType()), replacements).go(join.getCondition());

        call.transformTo(new CacheableIgniteHashJoin(join.getCluster(), join.getTraitSet(), join.getLeft(), newScan,
            newJoinCondition, join.getVariablesSet(), join.getJoinType(), scan.pushUpPredicate(), scanColumnFilter, scan.getRowType()));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {

        assert call.rel(0) instanceof IgniteHashJoin;
        assert call.rel(2) instanceof IgniteTableScan;

        ProjectableFilterableTableScan scan = call.rel(2);

        // We only push up predicates and required columns, not projects. For all of the TPC-H workloads projects
        // dont occur in a TableScan with a HashJoin above it. If it doesnt have a project we can cache and handle it
        return scan.projects() == null;
    }


    /**  */
    @SuppressWarnings({"ClassNameSameAsAncestorName", "PublicInnerClass"})
    @Value.Immutable
    public interface Config extends RelRule.Config {

        Config DEFAULT = ImmutableMakeHashJoinCachableRule.Config.of()
            .withOperandSupplier(b0 ->
                b0.operand(IgniteHashJoin.class)
                    .predicate(join -> !(join instanceof CacheableIgniteHashJoin))
                    .inputs(
                        b1 -> b1.operand(IgniteRel.class).anyInputs(),
                        b2 -> b2.operand(IgniteTableScan.class).noInputs()
                    ));

        @Override
        default MakeHashJoinCachableRule toRule() {
            return new MakeHashJoinCachableRule(this);
        }

    }

    private static class RexInputReplacer implements RexVisitor<RexNode> {

        final RexBuilder builder;
        final RelDataType newRelType;

        final HashMap<Integer, Integer> replacements;

        private RexInputReplacer(RelDataType newRelType, HashMap<Integer, Integer> replacements) {
            builder = RexUtils.builder(Commons.emptyCluster());
            this.newRelType = newRelType;
            this.replacements = replacements;
        }

        @Override
        public RexNode visitInputRef(RexInputRef ref) {
            if (replacements.containsKey(ref.getIndex()))
                return builder.makeInputRef(newRelType, replacements.get(ref.getIndex()));

            return ref;
        }

        @Override
        public RexNode visitLocalRef(RexLocalRef ref) {
            if (replacements.containsKey(ref.getIndex()))
                return builder.makeLocalRef(newRelType, replacements.get(ref.getIndex()));

            return ref;
        }

        @Override
        public RexNode visitLiteral(RexLiteral ref) {
            return ref;
        }

        @Override
        public RexNode visitCall(RexCall ref) {
            ArrayList<RexNode> newOperands = new ArrayList<>();
            for (RexNode operand : ref.getOperands()) {
                newOperands.add(operand.accept(this));
            }
            return builder.makeCall(ref.getOperator(), newOperands);
        }

        @Override
        public RexNode visitOver(RexOver rexOver) {
            return rexOver;
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
            return rexCorrelVariable;
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam rexDynamicParam) {
            return rexDynamicParam;
        }

        @Override
        public RexNode visitRangeRef(RexRangeRef rexRangeRef) {
            return rexRangeRef;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess rexFieldAccess) {
            return rexFieldAccess;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery rexSubQuery) {
            return rexSubQuery;
        }

        @Override
        public RexNode visitTableInputRef(RexTableInputRef ref) {
            return ref;
        }

        @Override
        public RexNode visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
            return rexPatternFieldRef;
        }

        public RexNode go(@Nullable RexNode rexNode) {
            return rexNode == null ? null : rexNode.accept(this);
        }
    }

}
