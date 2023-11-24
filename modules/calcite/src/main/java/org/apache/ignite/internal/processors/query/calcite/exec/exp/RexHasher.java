package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import com.google.common.collect.ImmutableList;
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
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/**
 * Converts a {@link RexNode REX expressions} to {@link String hash string} for a certain
 * Used in the HashJoin
 */

public class RexHasher<Row> implements RexVisitor<Object> {

    private final RowHandler<Row> handler;
    private final int offset;
    private final Row row;

    ImmutableList.Builder<Object> key = new ImmutableList.Builder<>();

    public RexHasher(RowHandler<Row> handler, int startIndex, Row row) {
        this.handler = handler;
        this.row = row;
        offset = startIndex * -1;
    }

    public RexHashKey go(RexNode cond) {
        cond.accept(this);
        ImmutableList<Object> finalKey = key.build();
        if (finalKey.isEmpty()) throw new Error("Unable to build RexHashKey!");
        return new RexHashKey(finalKey);
    }

    @Override
    public Object visitInputRef(RexInputRef node) {
        int index = offset + node.getIndex();
        if (index < 0 || index >= handler.columnCount(row)) return null;
        return handler.get(index, row);
    }

    @Override
    public Object visitLocalRef(RexLocalRef node) {
        int index = offset + node.getIndex();
        if (index < 0 || index >= handler.columnCount(row)) return null;
        return handler.get(index, row);
    }

    @Override
    public Object visitCall(RexCall node) {
        switch (node.getKind()) {
            case EQUALS:
                // Do something
                assert node.getOperands().size() == 2;
                Object left = node.getOperands().get(0).accept(this);
                Object right = node.getOperands().get(1).accept(this);
                if (left != null) key.add(left);
                if (right != null) key.add(right);
            case AND:
//                assert node.ope
            default:
                return null;
        }
    }

    /* ======================
     * The rest are TBD
     * ======================
     */

    @Override
    public Object visitLiteral(RexLiteral rexLiteral) {
        return null;
    }

    @Override
    public Object visitOver(RexOver rexOver) {
        return null;
    }

    @Override
    public Object visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
        return null;
    }

    @Override
    public Object visitDynamicParam(RexDynamicParam rexDynamicParam) {
        return null;
    }

    @Override
    public Object visitRangeRef(RexRangeRef rexRangeRef) {
        return null;
    }

    @Override
    public Object visitFieldAccess(RexFieldAccess rexFieldAccess) {
        return null;
    }

    @Override
    public Boolean visitSubQuery(RexSubQuery rexSubQuery) {
        return null;
    }

    @Override
    public Object visitTableInputRef(RexTableInputRef rexTableInputRef) {
        return null;
    }

    @Override
    public Object visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
        return null;
    }
}
