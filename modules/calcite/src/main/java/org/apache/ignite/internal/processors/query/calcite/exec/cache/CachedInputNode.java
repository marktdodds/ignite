package org.apache.ignite.internal.processors.query.calcite.exec.cache;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Downstream;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;

import java.util.List;

/**
 * This gets injected in place of an input whose results have been cached further upstream.
 * Upon the first request it auto-closes itself.
 *
 * @param <Row>
 */
public class CachedInputNode<Row> extends AbstractNode<Row> {
    /**  */
    public CachedInputNode(ExecutionContext<Row> ctx, RelDataType rowType) {
        super(ctx, rowType);
    }

    /**  */
    @Override
    protected void rewindInternal() {
        // No-op
    }

    /**  */
    @Override
    protected Downstream<Row> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<Row>> sources) {
        throw new UnsupportedOperationException();
    }

    /**
     * Requests next bunch of rows.
     */
    @Override
    public void request(int rowsCnt) throws Exception {
        downstream().end();
    }
}
