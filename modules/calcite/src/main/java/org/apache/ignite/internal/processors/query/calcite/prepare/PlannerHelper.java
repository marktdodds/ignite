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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.hint.HintUtils;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.util.InternalDebug;

/** */
public class PlannerHelper {
    /**
     * Default constructor.
     */
    private PlannerHelper() {

    }

    private static int maxNestedJoins(RelNode rel) {
        if (rel instanceof LogicalJoin) return 1 + Math.max(maxNestedJoins(rel.getInput(0)), maxNestedJoins(rel.getInput(1)));
        int max = 0;
        for (RelNode child : rel.getInputs()) {
            max = Math.max(max, maxNestedJoins(child));
        }
        return max;
    }

    private static int totalNestedJoins(RelNode rel) {
        if (rel instanceof LogicalJoin) return 1 + totalNestedJoins(rel.getInput(0)) + totalNestedJoins(rel.getInput(1));
        int sum = 0;
        for (RelNode child : rel.getInputs()) {
            sum += totalNestedJoins(child);
        }
        return sum;
    }

    /**
     * @param sqlNode Sql node.
     * @param planner Planner.
     * @param log Logger.
     */
    public static IgniteRel optimize(SqlNode sqlNode, IgnitePlanner planner, IgniteLogger log) {
        try {
            // Convert to Relational operators graph.
            RelRoot root = planner.rel(sqlNode);

            root = addExternalOptions(root);

            planner.setDisabledRules(HintUtils.options(root.rel, extractRootHints(root.rel), HintDefinition.DISABLE_RULE));

            RelNode rel = root.rel;

            // Transformation chain
            rel = planner.transform(PlannerPhase.HEP_DECORRELATE, rel.getTraitSet(), rel);

            // RelOptUtil#propagateRelHints(RelNode, equiv) may skip hints because current RelNode has no hints.
            // Or if hints reside in a child nodes which are not inputs of the current node. Like LogicalFlter#condition.
            // Such hints may appear or be required below in the tree, after rules applying.
            // In Calcite, RelDecorrelator#decorrelateQuery(...) can re-propagate hints.
            rel = RelOptUtil.propagateRelHints(rel, false);

            rel = planner.replaceCorrelatesCollisions(rel);

            rel = planner.trimUnusedFields(root.withRel(rel)).rel;

            rel = planner.transform(PlannerPhase.HEP_FILTER_PUSH_DOWN, rel.getTraitSet(), rel);

            rel = planner.transform(PlannerPhase.HEP_PROJECT_PUSH_DOWN, rel.getTraitSet(), rel);

            if (IgniteSystemProperties.getBoolean("MD_NEW_QUERY_PLANNER", false)) {

                ((RelMetadataQueryEx) rel.getCluster().getMetadataQuery()).setAllowNonIgniteCostFunctions(true);

                System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));
                rel = planner.transform(PlannerPhase.LOGICAL_OPTIMIZATIONS, rel.getTraitSet(), rel);
                System.out.println(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES));

                ((RelMetadataQueryEx) rel.getCluster().getMetadataQuery()).setAllowNonIgniteCostFunctions(false);
            }

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(root.collation == null ? RelCollations.EMPTY : root.collation)
                .simplify();

            IgniteRel igniteRel;

            if (IgniteSystemProperties.getBoolean("MD_NEW_QUERY_PLANNER", false)) {
                if (maxNestedJoins(rel) > 3 || totalNestedJoins(rel) > 4) igniteRel = planner.transform(PlannerPhase.PHYSICAL_OPTIMIZATION_NO_JOIN, desired, rel);
                else igniteRel = planner.transform(PlannerPhase.PHYSICAL_OPTIMIZATION, desired, rel);
            } else {
                igniteRel = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);
            }

            if (IgniteSystemProperties.getBoolean("MD_LOG_EXECUTION_PLANS", false)) {
                InternalDebug.alwaysLog(sqlNode.toString().replace("\r", "").replace("\n", " "));
                InternalDebug.alwaysLog(RelOptUtil.toString(igniteRel, SqlExplainLevel.ALL_ATTRIBUTES));
            }

            if (!root.isRefTrivial()) {
                final List<RexNode> projects = new ArrayList<>();
                final RexBuilder rexBuilder = igniteRel.getCluster().getRexBuilder();

                for (int field : Pair.left(root.fields))
                    projects.add(rexBuilder.makeInputRef(igniteRel, field));

                igniteRel = new IgniteProject(igniteRel.getCluster(), desired, igniteRel, projects, root.validatedRowType);
            }

            if (sqlNode.isA(ImmutableSet.of(SqlKind.INSERT, SqlKind.UPDATE, SqlKind.MERGE)))
                igniteRel = new FixDependentModifyNodeShuttle().visit(igniteRel);

            return igniteRel;
        }
        catch (Throwable ex) {
            log.error("Unexpected error at query optimizer.", ex);
            log.error(planner.dump());

            throw ex;
        }
    }

    /**
     * Add external options as hints to {@code root.rel}.
     *
     * @return New or old root node.
     */
    private static RelRoot addExternalOptions(RelRoot root) {
        if (!Commons.context(root.rel).isForcedJoinOrder())
            return root;

        if (!(root.rel instanceof Hintable)) {
            Commons.context(root.rel).logger().warning("Unable to set hint " + HintDefinition.ENFORCE_JOIN_ORDER
                + " passed as an external parameter to the root relation operator ["
                + RelOptUtil.toString(HintUtils.noInputsRelWrap(root.rel)).trim()
                + "] because it is not a Hintable.");

            return root;
        }

        List<RelHint> newHints = Stream.concat(HintUtils.allRelHints(root.rel).stream(),
            Stream.of(RelHint.builder(HintDefinition.ENFORCE_JOIN_ORDER.name()).build())).collect(Collectors.toList());

        root = root.withRel(((Hintable)root.rel).withHints(newHints));

        RelOptUtil.propagateRelHints(root.rel, false);

        return root;
    }

    /**
     * Extracts planner-level hints like 'DISABLE_RULE' if the root node is a combining node like 'UNION'.
     */
    private static Collection<RelHint> extractRootHints(RelNode rel) {
        if (!HintUtils.allRelHints(rel).isEmpty())
            return HintUtils.allRelHints(rel);

        if (rel instanceof SetOp) {
            return F.flatCollections(rel.getInputs().stream()
                .map(PlannerHelper::extractRootHints).collect(Collectors.toList()));
        }

        return Collections.emptyList();
    }

    /**
     * This shuttle analyzes a relational tree and inserts an eager spool node
     * just under the TableModify node in case latter depends upon a table used
     * to query the data for modify node to avoid the double processing
     * of the retrieved rows.
     * <p/>
     * It considers two cases: <ol>
     *     <li>
     *         Modify node produces rows to insert, then a spool is required.
     *     </li>
     *     <li>
     *         Modify node updates rows only, then a spool is required if 1) we
     *         are scaning an index and 2) any of the indexed column is updated
     *         by modify node.
     *     </li>
     * <ol/>
     *
     */
    private static class FixDependentModifyNodeShuttle extends IgniteRelShuttle {
        /**
         * Flag indicates whether we should insert a spool or not.
         */
        private boolean spoolNeeded;

        /** Current modify node. */
        private IgniteTableModify modifyNode;

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteTableModify rel) {
            assert modifyNode == null;

            modifyNode = rel;

            if (rel.isDelete())
                return rel;

            if (rel.isMerge()) // MERGE operator always contains modified table as a source.
                spoolNeeded = true;
            else
                processNode(rel);

            if (spoolNeeded) {
                IgniteTableSpool spool = new IgniteTableSpool(
                    rel.getCluster(),
                    rel.getInput().getTraitSet(),
                    Spool.Type.EAGER,
                    rel.getInput()
                );

                rel.replaceInput(0, spool);
            }

            return rel;
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteTableScan rel) {
            return processScan(rel);
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteIndexScan rel) {
            return processScan(rel);
        }

        /** {@inheritDoc} */
        @Override protected IgniteRel processNode(IgniteRel rel) {
            List<IgniteRel> inputs = Commons.cast(rel.getInputs());

            for (int i = 0; i < inputs.size(); i++) {
                if (spoolNeeded)
                    break;

                visitChild(rel, i, inputs.get(i));
            }

            return rel;
        }

        /**
         * Process a scan node and raise a {@link #spoolNeeded flag} if needed.
         *
         * @param scan TableScan to analize.
         * @return The input rel.
         */
        private IgniteRel processScan(TableScan scan) {
            IgniteTable tbl = modifyNode != null ? modifyNode.getTable().unwrap(IgniteTable.class) : null;

            if (tbl == null || scan.getTable().unwrap(IgniteTable.class) != tbl)
                return (IgniteRel)scan;

            if (modifyNodeInsertsData()) {
                spoolNeeded = true;

                return (IgniteRel)scan;
            }

            // for update-only node the spool needed if any of the updated
            // column is part of the index we are going to scan
            if (scan instanceof IgniteTableScan)
                return (IgniteRel)scan;

            ImmutableSet<Integer> indexedCols = ImmutableSet.copyOf(
                tbl.getIndex(((AbstractIndexScan)scan).indexName()).collation().getKeys());

            spoolNeeded = modifyNode.getUpdateColumnList().stream()
                .map(tbl.descriptor()::columnDescriptor)
                .map(ColumnDescriptor::fieldIndex)
                .anyMatch(indexedCols::contains);

            return (IgniteRel)scan;
        }

        /**
         * @return {@code true} in case {@link #modifyNode} produces any insert.
         */
        private boolean modifyNodeInsertsData() {
            return modifyNode.isInsert();
        }
    }
}
