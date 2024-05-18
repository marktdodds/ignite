package org.apache.ignite.internal.processors.query.calcite.prepare;

import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class NodeIdFragmentIdPair {
    private final UUID nodeId;
    private final long fragId;

    public NodeIdFragmentIdPair(UUID nodeId, long fragId) {
        this.nodeId = nodeId;
        this.fragId = fragId;
    }

    public UUID getNodeId() {
        return nodeId;
    }

    public long getFragId() {
        return fragId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, fragId);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NodeIdFragmentIdPair)) return false;
        NodeIdFragmentIdPair other = (NodeIdFragmentIdPair) obj;
        return other.nodeId.equals(nodeId) && other.fragId == fragId;
    }

    public static class MarshallableCollectionMessage implements CalciteMessage {
        /**  */
        private static final long serialVersionUID = 0L;

        @GridDirectCollection(long.class)
        private List<Long> fragIds;

        /**  */
        @GridDirectCollection(UUID.class)
        private List<UUID> nodeIds;

        /**
         * Empty constructor required for direct marshalling.
         */
        public MarshallableCollectionMessage() {
            // No-op.
        }

        /**
         * @param keys keys to wrap.
         */
        public MarshallableCollectionMessage(List<NodeIdFragmentIdPair> keys) {
            fragIds = new ArrayList<>();
            nodeIds = new ArrayList<>();
            for (NodeIdFragmentIdPair key : keys) {
                fragIds.add(key.getFragId());
                nodeIds.add(key.getNodeId());
            }
        }

        /**
         * @return The collection of UUIDs that was wrapped.
         */
        public List<NodeIdFragmentIdPair> keys() {
            assert fragIds.size() == nodeIds.size();
            List<NodeIdFragmentIdPair> keys = new ArrayList<>();
            for (int i = 0; i < fragIds.size(); i++) {
                keys.add(new NodeIdFragmentIdPair(nodeIds.get(i), fragIds.get(i)));
            }
            return keys;
        }

        /** {@inheritDoc} */
        @Override
        public void onAckReceived() {
            // No-op.
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
                    if (!writer.writeCollection("fragIds", fragIds, MessageCollectionItemType.LONG))
                        return false;

                    writer.incrementState();
                case 1:
                    if (!writer.writeCollection("nodeIds", nodeIds, MessageCollectionItemType.UUID))
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
                    fragIds = reader.readCollection("fragIds", MessageCollectionItemType.LONG);

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
                case 1:
                    nodeIds = reader.readCollection("nodeIds", MessageCollectionItemType.UUID);

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
            }

            return reader.afterMessageRead(MarshallableCollectionMessage.class);
        }

        /**
         * @return Message type.
         */
        @Override
        public MessageType type() {
            return MessageType.FRAGMENT_REMOTE_KEY_COLLATION;
        }

        /** {@inheritDoc} */
        @Override
        public byte fieldsCount() {
            return 2;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            MarshallableCollectionMessage that = (MarshallableCollectionMessage) o;

            return fragIds.equals(that.fragIds) && nodeIds.equals(that.nodeIds);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(nodeIds, fragIds);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(MarshallableCollectionMessage.class, this, "entries", fragIds == null ? '0' : fragIds.size());
        }
    }


}
