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

import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodeMappingException;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.externalize.RelJsonWriter.toJson;

/**
 * Fragment of distributed query
 */
public class Fragment {
    /** */
    private final long id;

    /** */
    private final IgniteRel root;

    /** Serialized root representation. */
    @GridToStringExclude
    private final String rootSer;

    /** */
    private final FragmentMapping mapping;

    /** */
    private final ImmutableList<IgniteReceiver> receivers;

    private final Collection<Long> receiverExchangeIds;

    /** */
    private final int totalVariants;

    /** */
    private final int variantId;

    /**
     * @param id Fragment id.
     * @param root Root node of the fragment.
     * @param receivers Remote sources of the fragment.
     */
    public Fragment(long id, IgniteRel root, List<IgniteReceiver> receivers) {
        this(id, root, receivers, null, null, 1, 0);
    }

    /** */
    Fragment(long id, IgniteRel root, List<IgniteReceiver> receivers, @Nullable String rootSer, @Nullable FragmentMapping mapping, int totalVariants, int variantId) {
        this.id = id;
        this.root = root;
        this.receivers = ImmutableList.copyOf(receivers);
        this.receiverExchangeIds = receivers.stream().map(IgniteReceiver::exchangeId).collect(Collectors.toSet());
        this.rootSer = rootSer != null ? rootSer : toJson(root);
        this.mapping = mapping;
        this.totalVariants = totalVariants;
        this.variantId = variantId;
    }



    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return id;
    }

    /**
     * @return Root node.
     */
    public IgniteRel root() {
        return root;
    }

    /**
     * Lazy serialized root representation.
     *
     * @return Serialized form.
     */
    public String serialized() {
        return rootSer;
    }

    /** */
    public FragmentMapping mapping() {
        return mapping;
    }

    /**
     * @return Fragment remote sources.
     */
    public List<IgniteReceiver> receivers() {
        return receivers;
    }

    /**
     * Whether the fragment is a receiver for an exchange. Logically equivalent to
     * @returns boolean logically equivalent to {@code receivers().anyMatch(r -> r.exchangeId() == exchangeId)}
     */
    public boolean isReceiverForExchange(long exchangeId) {
        return receiverExchangeIds.contains(exchangeId);
    }

    /** */
    public boolean rootFragment() {
        return !(root instanceof IgniteSender);
    }

    /** */
    public int totalVariants() {
        return totalVariants;
    }

    /** */
    public int variantId() {
        return variantId;
    }

    /** */
    public Fragment attach(RelOptCluster cluster) {
        return root.getCluster() == cluster ? this : new Cloner(cluster).go(this);
    }

    /** */
    public Fragment filterByPartitions(int[] parts) throws ColocationMappingException {
        return new Fragment(id, root, receivers, rootSer, mapping.filterByPartitions(parts), totalVariants, variantId);
    }

    /**
     * Updates the rel of a Fragment. Used to create multithreading plans where
     * fragments are duplicated and small changes are made to the execution tree.
     * @return A fragment with a new id and the new root. Remotes and rootSer are uninitialized and left for caller
     */
    public Fragment multiThreaded(IgniteRel newRoot, List<IgniteReceiver> remotes, int totalVariants, int variantId) {
        return new Fragment(IdGenerator.nextId(), newRoot, remotes, "", mapping, totalVariants, variantId);
    }

    /**
     * Copies the fragment but reloads the serialization in cases where the referenced { root } has changed
     */
    public Fragment redoSerialization() {
        return new Fragment(id, root, receivers, null, mapping, totalVariants, variantId);
    }

    /**
     * Mapps the fragment to its data location.
     * @param ctx Planner context.
     * @param mq Metadata query.
     */
    Fragment map(MappingService mappingSrvc, MappingQueryContext ctx, RelMetadataQuery mq) throws FragmentMappingException {
        if (mapping != null)
            return this;

        return new Fragment(id, root, receivers, rootSer, mapping(ctx, mq, nodesSource(mappingSrvc, ctx)), totalVariants, variantId);
    }

    /**
     * Remap the fragment to a new mapping
     * @param mapping The new mapping to apply
     */
    Fragment remap(FragmentMapping mapping) {
        return new Fragment(id, root, receivers, rootSer, mapping, totalVariants, variantId);
    }

    /** */
    private FragmentMapping mapping(MappingQueryContext ctx, RelMetadataQuery mq, Supplier<List<UUID>> nodesSource) {
        try {
            FragmentMapping mapping = IgniteMdFragmentMapping._fragmentMapping(root, mq, ctx);

            if (rootFragment()) {
                if (ctx.isLocal())
                    mapping = mapping.local(ctx.localNodeId());
                else
                    mapping = FragmentMapping.create(ctx.localNodeId()).colocate(mapping);
            }

            if (single() && mapping.nodeIds().size() > 1) {
                // this is possible when the fragment contains scan of a replicated cache, which brings
                // several nodes (actually all containing nodes) to the colocation group, but this fragment
                // supposed to be executed on a single node, so let's choose one wisely
                mapping = FragmentMapping.create(mapping.nodeIds()
                    .get(ThreadLocalRandom.current().nextInt(mapping.nodeIds().size()))).colocate(mapping);
            }

            return mapping.finalizeMapping(nodesSource);
        }
        catch (NodeMappingException e) {
            throw new FragmentMappingException("Failed to calculate physical distribution", this, e.node(), e);
        }
        catch (ColocationMappingException e) {
            throw new FragmentMappingException("Failed to calculate physical distribution", this, root, e);
        }
    }

    /** */
    @NotNull private Supplier<List<UUID>> nodesSource(MappingService mappingSrvc, MappingQueryContext ctx) {
        return () -> mappingSrvc.executionNodes(ctx.topologyVersion(), single(), null);
    }

    /** */
    protected boolean single() {
		return (root instanceof IgniteSender ? ((IgniteSender) root).sourceDistribution() : root.distribution()).satisfies(IgniteDistributions.single());
	}

    /**
     * Get a list of caches used by a fragment
     * @return List of cache names as Strings
     */
    protected List<IgniteCacheTable> cachesUsed() {
        Deque<RelNode> stack = new LinkedList<>();
        List<IgniteCacheTable> caches = new LinkedList<>();
        stack.add(root());

        while (!stack.isEmpty()) {
            RelNode r = stack.pop();

            // Add children
            stack.addAll(r.getInputs());

            // Check table
            RelOptTable table = r.getTable();
            if (table == null) continue;
            Optional<IgniteCacheTable> cacheTable = table.maybeUnwrap(IgniteCacheTable.class);
            if (!cacheTable.isPresent()) continue;

            // Add cache name if present
            caches.add(cacheTable.get());
        }
        return caches;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Fragment.class, this, "root", RelOptUtil.toString(root));
    }
}
