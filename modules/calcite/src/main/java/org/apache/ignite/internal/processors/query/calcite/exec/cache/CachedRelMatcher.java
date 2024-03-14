package org.apache.ignite.internal.processors.query.calcite.exec.cache;

import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCollect;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteDistributedMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteDistributedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexBound;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexCount;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.cache.CacheableIgniteHashJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSetOp;

class CachedRelMatcher implements IgniteRelVisitor<Boolean> {
    IgniteRel other;

    CachedRelMatcher(IgniteRel other) {
        this.other = other;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteSender rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteFilter rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteTrimExchange rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteProject rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteNestedLoopJoin rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteDistributedNestedLoopJoin rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteHashJoin rel) {
        if (!rel.cacheMatches(other)) return false;
        other = (IgniteRel) other.getInput(1);
        return visit((IgniteRel) rel.getRight());
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(CacheableIgniteHashJoin rel) {
        return visit((IgniteHashJoin) rel);
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteDistributedMergeJoin rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteCorrelatedNestedLoopJoin rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteMergeJoin rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteIndexScan rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteIndexCount rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteIndexBound rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteTableScan rel) {
        return rel.cacheMatches(other);
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteReceiver rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteExchange rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteColocatedHashAggregate rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteMapHashAggregate rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteReduceHashAggregate rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteColocatedSortAggregate rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteMapSortAggregate rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteReduceSortAggregate rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteTableModify rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteValues rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteUnionAll rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteSort rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteTableSpool rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteSortedIndexSpool rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteLimit rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteHashIndexSpool rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteSetOp rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteTableFunctionScan rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteCollect rel) {
        return false;
    }

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    @Override
    public Boolean visit(IgniteRel rel) {
        return rel.accept(this);
    }
}
