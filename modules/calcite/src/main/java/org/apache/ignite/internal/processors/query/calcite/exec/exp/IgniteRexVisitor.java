package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;

public abstract class IgniteRexVisitor<T> implements RexVisitor<T> {

    public abstract T defaultValue();

    @Override
    public T visitInputRef(RexInputRef inputRef) {
        return defaultValue();
    }

    @Override
    public T visitLocalRef(RexLocalRef localRef) {
        return defaultValue();
    }

    @Override
    public T visitLiteral(RexLiteral literal) {
        return defaultValue();
    }

    @Override
    public T visitCall(RexCall call) {
        return defaultValue();
    }

    @Override
    public T visitOver(RexOver over) {
        return defaultValue();
    }

    @Override
    public T visitCorrelVariable(RexCorrelVariable correlVariable) {
        return defaultValue();
    }

    @Override
    public T visitDynamicParam(RexDynamicParam dynamicParam) {
        return defaultValue();
    }

    @Override
    public T visitRangeRef(RexRangeRef rangeRef) {
        return defaultValue();
    }

    @Override
    public T visitFieldAccess(RexFieldAccess fieldAccess) {
        return defaultValue();
    }

    @Override
    public T visitSubQuery(RexSubQuery subQuery) {
        return defaultValue();
    }

    @Override
    public T visitTableInputRef(RexTableInputRef fieldRef) {
        return defaultValue();
    }

    @Override
    public T visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return defaultValue();
    }
}
