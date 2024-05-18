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

package org.apache.ignite.internal.processors.query.calcite.message;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 *
 */
public class QueryBatchMessage implements MarshalableMessage, ExecutionContextAware {

    /**  */
    private UUID qryId;

    private long outboxFragmentId;

    /**  */
    private long exchangeId;

    /**  */
    private int batchId;

    /**  */
    private boolean last;

    /**  */
    private int[] marshallableType;

    /**  */
    @GridDirectTransient
    private List<Object> rows;

    /**  */
    @GridDirectCollection(ValueMessage.class)
    private List<ValueMessage> mRows;

    /**  */
    public QueryBatchMessage() {
    }

    /**  */
    public QueryBatchMessage(UUID qryId, long outboxFragmentId, long exchangeId, int batchId, boolean last, List<Object> rows, RelDataType rowType) {
        this.qryId = qryId;
        this.outboxFragmentId = outboxFragmentId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.last = last;
        this.rows = rows;
        this.marshallableType = SmartRowMessage.marshallableTypeFromRowType(rowType);
    }

    /** {@inheritDoc} */
    @Override
    public UUID queryId() {
        return qryId;
    }

    /** {@inheritDoc} */
    @Override
    public long executorFragmentId() {
        return exchangeId;
    }

    public long outboxFragmentId() {
        return outboxFragmentId;
    }

    /**
     * @return Exchange ID.
     */
    public long exchangeId() {
        return exchangeId;
    }

    /**
     * @return Batch ID.
     */
    public int batchId() {
        return batchId;
    }

    /**
     * @return Last batch flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Rows.
     */
    public List<Object> rows() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override
    public void prepareMarshal(MarshallingContext ctx) throws IgniteCheckedException {
        if (mRows != null || rows == null)
            return;

        mRows = new ArrayList<>(rows.size());

        for (Object row : rows) {
            ValueMessage mRow = CalciteMessageFactory.asMessage(row, marshallableType);

            assert mRow != null;

            mRow.prepareMarshal(ctx);

            mRows.add(mRow);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void prepareUnmarshal(MarshallingContext ctx) throws IgniteCheckedException {
        if (rows != null || mRows == null)
            return;

        rows = new ArrayList<>(mRows.size());

        for (ValueMessage mRow : mRows) {
            assert mRow != null;

            if (mRow instanceof SmartRowMessage) ((SmartRowMessage) mRow).setMarshallableType(marshallableType);

            mRow.prepareUnmarshal(ctx);

            rows.add(mRow.value());
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt("batchId", batchId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("exchangeId", exchangeId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeBoolean("last", last))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeCollection("mRows", mRows, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeUuid("queryId", qryId))
                    return false;

                writer.incrementState();


            case 5:
                if (!writer.writeIntArray("marshallableType", marshallableType))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("outboxFragmentId", outboxFragmentId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                batchId = reader.readInt("batchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                exchangeId = reader.readLong("exchangeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                last = reader.readBoolean("last");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                mRows = reader.readCollection("mRows", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                qryId = reader.readUuid("queryId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                marshallableType = reader.readIntArray("marshallableType");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                outboxFragmentId = reader.readLong("outboxFragmentId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(QueryBatchMessage.class);
    }

    /** {@inheritDoc} */
    @Override
    public MessageType type() {
        return MessageType.QUERY_BATCH_MESSAGE;
    }

    /** {@inheritDoc} */
    @Override
    public byte fieldsCount() {
        return 7;
    }
}
