package org.apache.ignite.internal.processors.query.calcite.message;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;


public class SmartRowMessage implements ValueMessage {

    public static final int[] EMPTY = null;

    /**  */
    @GridDirectTransient
    private Object val;

    /**  */
    private byte[] serialized;
    private int[] marshallableType;

    /**  */
    public SmartRowMessage() {

    }

    /**  */
    public SmartRowMessage(Object val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override
    public Object value() {
        return val;
    }


    /**
     * Set the marshallable type to be used in marshalling/unmarshalling
     *
     * @param marshallableType
     */
    public void setMarshallableType(int[] marshallableType) {
        this.marshallableType = marshallableType;
    }


    /** {@inheritDoc} */
    @Override
    public void prepareMarshal(MarshallingContext ctx) throws IgniteCheckedException {
        prepareMarshal();
    }

    public void prepareMarshal() throws IgniteCheckedException {
        assert marshallableType != null;
        if (val == null || serialized != null) return;

        int length = 0;
        for (int type : marshallableType) {
            if (SmartRowMessageObjectType.Integer.equals(type)) {
                length += Integer.BYTES;
            }
        }

        serialized = new byte[length];
        Object[] list = (Object[]) val;
        int c = 0;
        int i = 0;
        for (int type : marshallableType) {
            if (SmartRowMessageObjectType.Integer.equals(type)) {
                for (byte b : ByteBuffer.allocate(Integer.BYTES).putInt((Integer) list[i++]).array()) serialized[c++] = b;
            } else {
                throw new Error("Unknown marshallable type: " + type);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void prepareUnmarshal(MarshallingContext ctx) throws IgniteCheckedException {
        assert marshallableType != null;
        if (val != null || serialized == null) return;

        Object[] list = new Object[marshallableType.length];
        int index = 0;
        int offset = 0;
        for (int type : marshallableType) {
            if (SmartRowMessageObjectType.Integer.equals(type)) {
                list[index++] = ByteBuffer.wrap(serialized, offset, Integer.BYTES).getInt();
                offset += Integer.BYTES;
            } else {
                throw new Error("Unknown marshallable type: " + type);
            }
        }

        val = list;
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
                if (!writer.writeByteArray("serialized", serialized))
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
                serialized = reader.readByteArray("serialized");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GenericValueMessage.class);
    }

    /** {@inheritDoc} */
    @Override
    public MessageType type() {
        return MessageType.SMART_ROW_MESSAGE;
    }

    /** {@inheritDoc} */
    @Override
    public byte fieldsCount() {
        return 1;
    }

    /**
     * Create a marshallable type (list of integers) to be passed around for marshalling/unmarshalling
     *
     * @param rowType The SQL Record row type
     */
    public static int[] marshallableTypeFromRowType(RelDataType rowType) {
        if (rowType == null) return EMPTY;
        if (!IgniteSystemProperties.getBoolean("MD_SMART_MARSHALLING", false)) return EMPTY;

        int[] marshallableType = new int[rowType.getFieldCount()];
        int i = 0;
        for (RelDataTypeField field : rowType.getFieldList()) {
            switch (field.getType().getSqlTypeName()) {
                case INTEGER:
                    marshallableType[i++] = SmartRowMessageObjectType.Integer.ordinal();
                    break;
                default:
                    return EMPTY;
            }

        }
        return marshallableType;
    }

}

