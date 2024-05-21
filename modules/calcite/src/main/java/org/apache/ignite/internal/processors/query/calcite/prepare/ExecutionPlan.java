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

import com.google.common.collect.ImmutableList;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.InboxController.SourceControlType;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 *
 */
public class ExecutionPlan {
    /**  */
    private final AffinityTopologyVersion ver;

    /**  */
    private ImmutableList<Fragment> fragments;

    /**  */
    private final ImmutableList<PartitionNode> partNodes;

    /**  */
    ExecutionPlan(AffinityTopologyVersion ver, List<Fragment> fragments, List<PartitionNode> partNodes) {
        this.ver = ver;
        this.fragments = ImmutableList.copyOf(fragments);
        this.partNodes = ImmutableList.copyOf(partNodes);
    }

    /**  */
    public AffinityTopologyVersion topologyVersion() {
        return ver;
    }

    /**  */
    public List<Fragment> fragments() {
        return fragments;
    }

    /**  */
    public List<PartitionNode> partitionNodes() {
        return partNodes;
    }

    /**  */
    public FragmentMapping mapping(Fragment fragment) {
        return fragment.mapping();
    }

    /**  */
    public ColocationGroup target(Fragment fragment) {
        if (fragment.rootFragment())
            return null;

        IgniteSender sender = (IgniteSender) fragment.root();

        // Since an inbox can have multiple variant fragments, we cant key on targetFragment anymore. But
        // each variant fragment shares the same base properties (specifically its target nodes) so we only
        // need to find one of them and we can use that.
        Fragment target = fragments().stream()
            .filter(f -> f.isReceiverForExchange(sender.exchangeId()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Cannot find fragment with corresponding exchange ID. [" +
                "exchangeId=" + sender.exchangeId() + ", " + "fragments=" + fragments().stream().map(Fragment::serialized).collect(Collectors.toSet()) + "]"));

        return mapping(target).findGroup(sender.exchangeId());
    }

    /**  */
    public Map<Long, List<NodeIdFragmentIdPair>> remotes(Fragment fragment) {
        List<IgniteReceiver> remotes = fragment.receivers();

        if (F.isEmpty(remotes))
            return null;

        HashMap<Long, List<NodeIdFragmentIdPair>> res = U.newHashMap(remotes.size());

        for (IgniteReceiver remote : remotes) {
            List<NodeIdFragmentIdPair> senders = new ArrayList<>();
            for (Long senderId : remote.senderFragmentIds())
                for (UUID uuid : mapping(senderId).nodeIds())
                    senders.add(new NodeIdFragmentIdPair(uuid, senderId));
            res.put(remote.exchangeId(), senders);
        }
        return res;
    }

    public final int THREADING_COUNT = IgniteSystemProperties.getInteger("MD_THREADED_EXECUTION_WORKER_CNT", 1);

    public static final boolean ENABLED = IgniteSystemProperties.getBoolean("MD_THREADED_EXECUTION_PLANS", false);

    /**
     * Attempts to inject multithreaded fragments in place of single threaded ones.
     * I.e. a TableScan can be split into a multithreaeded table scan each scanning a subsection of partitions.
     *
     * @return A multithreaded plan, or the original plan if disabled.
     */
    public ExecutionPlan makeMultiThreaded() {
        if (!ENABLED || THREADING_COUNT == 1) return this;
        List<Fragment> newFrags = new ArrayList<>();
        Map<Long, List<IgniteReceiver>> receivers = new HashMap<>();

        for (Fragment original : fragments()) {
            List<Fragment> updatedFragments = new LinkedList<>();
            if (original.rootFragment()) {
                // Don't mess with the root fragment... for now...
                updatedFragments.add(original);
            } else {
                // Should be working with a Sender.
                assert original.root() instanceof IgniteSender;

                // Create the new plans if possible
                for (int i = 0; i < THREADING_COUNT; i++) {

                    IgniteRel multiThreadedRel = new MultiThreadingReplacer(SourceControlType.SPLITTER).go(original.root());
                    if (multiThreadedRel == null) {
                        updatedFragments = F.asList(original);
                        break;
                    }

                    // We set rootSer to avoid unnecessary serialization since we re-serialize everything at the end.
                    updatedFragments.add(original.multiThreaded(multiThreadedRel, getListOfReceivers(multiThreadedRel), THREADING_COUNT, i));
                }
            }

            // Add the newly created remotes to the list of receivers
            updatedFragments.forEach(fragment -> fragment.receivers().forEach(receiver -> {
                receiver.clearSenderFragmentIds(); // We'll reset these below once we've finished everything
                receivers.computeIfAbsent(receiver.exchangeId(), (k) -> new LinkedList<>()).add(receiver);
            }));

            newFrags.addAll(updatedFragments);
        }

        assert newFrags.size() >= fragments().size();

        // Update the sender fragment ids in the receivers
        newFrags.forEach(frag -> {
            if (frag.rootFragment()) return;
            long exchangeId = ((IgniteSender) frag.root()).exchangeId();
            receivers.getOrDefault(exchangeId, Collections.emptyList()).forEach(r -> r.addSenderFragmentId(frag.fragmentId()));
        });

        return copyWith(newFrags.stream().map(Fragment::redoSerialization).collect(Collectors.toList()));
    }

    /**
     * Recursively goes through an IgniteRel adding any IgniteReceivers it finds to the provided list.
     */
    private List<IgniteReceiver> getListOfReceivers(IgniteRel rel) {
        if (rel instanceof IgniteReceiver) return F.asList(((IgniteReceiver) rel));
        List<IgniteReceiver> res = new LinkedList<>();
        for (IgniteRel child : Commons.<IgniteRel>cast(rel.getInputs())) res.addAll(getListOfReceivers(child));
        return res;
    }

    /**
     * Copies the current execution plan, updating the fragment list.
     */
    private ExecutionPlan copyWith(List<Fragment> fragments) {
        return new ExecutionPlan(ver, fragments, partNodes);
    }

    /**  */
    private FragmentMapping mapping(long fragmentId) {
        return fragments().stream()
            .filter(f -> f.fragmentId() == fragmentId)
            .findAny().orElseThrow(() -> new IllegalStateException("Cannot find fragment with given ID. [" +
                "fragmentId=" + fragmentId + ", " + "fragments=" + fragments() + "]"))
            .mapping();
    }


    private static class MultiThreadingReplacer extends IgniteRelShuttle {

        private final SourceControlType type;

        private MultiThreadingReplacer(SourceControlType type) {
            this.type = type;
        }

        private IgniteRel isAllowed(IgniteRel rel) {
            return process(rel.clone(rel.getCluster(), Commons.cast(rel.getInputs())));
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteSender rel) {
            return isAllowed(rel);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteTableScan rel) {
            assert rel.getInputs().size() == 0;
            return rel.clone(type == SourceControlType.SPLITTER);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteIndexScan rel) {
            assert rel.getInputs().size() == 0;
            return rel.clone(type == SourceControlType.SPLITTER);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteReceiver rel) {
            return rel.clone(type);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteNestedLoopJoin rel) {
            IgniteRel left = new MultiThreadingReplacer(SourceControlType.DUPLICATOR).go((IgniteRel) rel.getLeft());
            if (left == null) return null;
            IgniteRel right = ((IgniteRel) rel.getRight()).accept(this);
            if (right == null) return null;
            return rel.clone(rel.getCluster(), F.asList(left, right));
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteSort rel) {
            return isAllowed(rel);
        }

        /** {@inheritDoc} */
        @Override
        public IgniteRel visit(IgniteMapSortAggregate rel) {
            return isAllowed(rel);
        }

        /**
         * Default value, if we find a child not enabled it won't do threading injection.
         */
        @Override
        protected IgniteRel processNode(IgniteRel rel) {
            return null;
        }

        /**
         * Visits all children of a parent.
         */
        private IgniteRel process(IgniteRel rel) {
            List<IgniteRel> inputs = Commons.cast(rel.getInputs());

            for (int i = 0; i < inputs.size(); i++)
                if (!visitChildNonNull(rel, i, inputs.get(i))) return null;

            return rel;
        }

        /**
         * Visits a child, returning false if the replacement child is null.
         */
        protected boolean visitChildNonNull(IgniteRel parent, int i, IgniteRel child) {
            IgniteRel newChild = visit(child);

            if (newChild == null) return false;

            if (newChild != child)
                parent.replaceInput(i, newChild);

            return true;
        }

        /**
         * Recurses through a tree
         *
         * @param executionRoot
         * @return
         */
        public IgniteRel go(IgniteRel executionRoot) {
            return executionRoot.accept(this);
        }
    }

    public static class MultiThreadingConfig {

        int nextThreadId = 0;
        final int maxThreadCount;

        public MultiThreadingConfig(int maxThreadCount) {
            this.maxThreadCount = maxThreadCount;
        }

        public MultiThreadingConfig copy() {
            return new MultiThreadingConfig(maxThreadCount);
        }

        public int getNextThreadId() {
            assert nextThreadId < maxThreadCount;
            return nextThreadId++;
        }

        public int maxThreadCount() {
            return maxThreadCount;
        }
    }
}
