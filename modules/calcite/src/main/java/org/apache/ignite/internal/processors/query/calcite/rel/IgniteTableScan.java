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
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * Relational operator that returns the contents of a table.
 */
public class IgniteTableScan extends ProjectableFilterableTableScan implements SourceAwareIgniteRel {
    /**  */
    private final long sourceId;

    /**
     * Boolean indicating whether this node is part of a variant set and should split its source
     */
    private final boolean isVariantSplitter;

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteTableScan(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));

        Object srcIdObj = input.get("sourceId");
        if (srcIdObj != null)
            sourceId = ((Number) srcIdObj).longValue();
        else
            sourceId = -1;

        isVariantSplitter = input.getBoolean("isVariantSplitter", false);
    }

    /**
     * Creates a TableScan.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param traits  Traits of this relational expression
     * @param tbl     Table definition.
     */
    public IgniteTableScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl
    ) {
        this(cluster, traits, tbl, null, null, null);
    }

    /**
     * Creates a TableScan.
     *
     * @param cluster         Cluster that this relational expression belongs to
     * @param traits          Traits of this relational expression
     * @param tbl             Table definition.
     * @param proj            Projects.
     * @param cond            Filters.
     * @param requiredColunms Participating colunms.
     */
    public IgniteTableScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColunms
    ) {
        this(-1L, cluster, traits, tbl, proj, cond, requiredColunms, false);
    }

    /**
     * Creates a TableScan.
     *
     * @param cluster           Cluster that this relational expression belongs to
     * @param traits            Traits of this relational expression
     * @param tbl               Table definition.
     * @param proj              Projects.
     * @param cond              Filters.
     * @param requiredColumns   Participating colunms.
     * @param isVariantSplitter
     */
    private IgniteTableScan(
        long sourceId,
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns,
        boolean isVariantSplitter) {
        super(cluster, traits, ImmutableList.of(), tbl, proj, cond, requiredColumns);
        this.sourceId = sourceId;
        this.isVariantSplitter = isVariantSplitter;
    }

    /**  */
    @Override
    public long sourceId() {
        return sourceId;
    }

    /**  */
    @Override
    protected RelWriter explainTerms0(RelWriter pw) {
        return super.explainTerms0(pw)
            .item("isVariantSplitter", isVariantSplitter)
            .itemIf("sourceId", sourceId, sourceId != -1);
    }

    /** {@inheritDoc} */
    @Override
    public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public IgniteTableScan clone(@Nullable RexNode condition, @Nullable ImmutableBitSet requiredColumns) {
        return new IgniteTableScan(sourceId, getCluster(), traitSet, table, projects, condition, requiredColumns, isVariantSplitter);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(long sourceId) {
        return new IgniteTableScan(sourceId, getCluster(), getTraitSet(), getTable(), projects, condition, requiredColumns, isVariantSplitter);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteTableScan(sourceId, cluster, getTraitSet(), getTable(), projects, condition, requiredColumns, isVariantSplitter);
    }

    /**  */
    public IgniteTableScan clone(boolean isVariantSplitter) {
        return new IgniteTableScan(sourceId, getCluster(), getTraitSet(), getTable(), projects, condition, requiredColumns, isVariantSplitter);
    }

    public boolean isVariantSplitter() {
        return isVariantSplitter;
    }

    @Override
    public boolean cacheMatches(IgniteRel other) {
        if (!(other instanceof IgniteTableScan)) return false;
        IgniteTableScan otherScan = (IgniteTableScan) other;

        return Objects.equals(projects(), otherScan.projects())
            && Objects.equals(requiredColumns(), otherScan.requiredColumns())
            && Objects.equals(condition(), otherScan.condition())
            && Objects.deepEquals(getTable().getQualifiedName(), other.getTable().getQualifiedName())
            ;
    }
}
