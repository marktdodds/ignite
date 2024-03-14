package org.apache.ignite.internal.processors.query.calcite.rel.cache;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.HashJoinNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;

public class CacheableIgniteHashJoin extends IgniteHashJoin {

    private HashJoinNode cachedExecutionNode;

    public CacheableIgniteHashJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                                   RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, null, null);
    }


    public CacheableIgniteHashJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                                   RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType,
                                   RexNode rightFilterCondition, ImmutableBitSet rightRequiredColumns) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, rightFilterCondition, rightRequiredColumns);
    }

    /**  */
    public CacheableIgniteHashJoin(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs().get(0),
            input.getInputs().get(1),
            input.getExpression("condition"),
            ImmutableSet.copyOf(Commons.transform(input.getIntegerList("variablesSet"), CorrelationId::new)),
            input.getEnum("joinType", JoinRelType.class));
    }

    public void setCachedExecutionNode(HashJoinNode n) {
        assert cachedExecutionNode == null;
        cachedExecutionNode = n;
    }

    public @Nullable HashJoinNode getCachedExecutionNode() {
        return cachedExecutionNode;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("usingCachedNode", cachedExecutionNode != null);
    }

    /** {@inheritDoc} */
    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
                     boolean semiJoinDone) {
        return new CacheableIgniteHashJoin(getCluster(), traitSet, left, right, condition, variablesSet, joinType, getRightFilterCondition(), getRightRequiredColumns());
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new CacheableIgniteHashJoin(cluster, getTraitSet(), inputs.get(0), inputs.get(1), getCondition(),
            getVariablesSet(), getJoinType(), getRightFilterCondition(), getRightRequiredColumns());
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deriveRowType() {
        RelDataType rightTableDerivedType = right.getTable().unwrap(IgniteTable.class).getRowType(Commons.typeFactory(getCluster()), getRightRequiredColumns());
        return SqlValidatorUtil.deriveJoinRowType(left.getRowType(), rightTableDerivedType, joinType, getCluster().getTypeFactory(), null, getSystemFieldList());
    }

}
