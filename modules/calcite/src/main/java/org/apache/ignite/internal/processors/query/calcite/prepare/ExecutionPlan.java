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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdFragmentMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.util.InternalDebug;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
class ExecutionPlan {
    /**  */
    private final AffinityTopologyVersion ver;

    /**  */
    private final ImmutableList<Fragment> fragments;

    /**  */
    ExecutionPlan(AffinityTopologyVersion ver, List<Fragment> fragments) {
        this.ver = ver;
        this.fragments = ImmutableList.copyOf(fragments);
    }

    /**  */
    public AffinityTopologyVersion topologyVersion() {
        return ver;
    }

    /**  */
    public List<Fragment> fragments() {
        return fragments;
    }

    private static class SiteOption implements Comparable<SiteOption> {

        private final UUID nodeId;
        private final int priority;

        // CPU load as % of total capacity
        private final double cpuLoad;

        private SiteOption(UUID nodeId, int priority, double cpuLoad) {
            this.nodeId = nodeId;
            this.priority = priority;
            this.cpuLoad = cpuLoad;
        }

        @Override
        public int compareTo(@NotNull ExecutionPlan.SiteOption o) {
            // within a 5% buffer we take the priority into account
            if (priority < o.priority) {
                return Double.compare(cpuLoad * 1.1, o.cpuLoad);
            } else if (priority > o.priority) {
                return Double.compare(o.cpuLoad * 1.1, cpuLoad);
            }
            return Double.compare(cpuLoad, o.cpuLoad);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof SiteOption && compareTo((SiteOption) o) == 0;
        }

        @Override
        public String toString() {
            return "{id: " + nodeId + ", cpuLoad: " + cpuLoad + ", priority: " + priority + "}";
        }
    }

    public ExecutionPlan optimize(MappingService mappingService, MappingQueryContext ctx, GridCacheProcessor gcp, GridDiscoveryManager gdm) {
        RelMetadataQuery mq = ctx.cluster().getMetadataQuery();

        // LinkedHashMap maintained order so we can recreate the Fragments list later
        LinkedHashMap<Long, Fragment> fragmentMap = new LinkedHashMap<>();
        Map<Long, List<IgniteCacheTable>> fragmentCaches = new HashMap<>();
        Map<String, CacheMetrics> cacheMetrics = new HashMap<>();

        fragments().forEach(f -> {
            // This ensures we are dealing with fresh fragments and won't mess up any other cached values
            fragmentMap.put(f.fragmentId(), f.remap(f.mapping()));
            List<IgniteCacheTable> cachesUsed = f.cachesUsed();
            fragmentCaches.put(f.fragmentId(), cachesUsed);
            cachesUsed.forEach(cache -> {
                String cacheName = cache.descriptor().cacheInfo().name();
                if (!cacheMetrics.containsKey(cacheName)) cacheMetrics.put(cacheName, gcp.cache(cacheName).clusterMetrics());
            });
        });

        // Single is false so we get all nodes, otherwise it will return a single random node.
        Supplier<List<UUID>> executionNodes = () -> mappingService.executionNodes(ctx.topologyVersion(), false, null);

        for (Fragment f : fragmentMap.values()) {
            if (f.root() instanceof IgniteReceiver || !f.single()) continue;
            assert f.mapping().nodeIds().size() == 1;

            FragmentMapping rootMapping = IgniteMdFragmentMapping._fragmentMapping(f.root(), mq, ctx);

//            System.out.printf("[Fragment %s] Est. Res Set: %s\n", f.fragmentId(), mq.getRowCount(f.root()));
            Map<UUID, SiteOption> siteOptions = new HashMap<>();
            UUID currentSite = f.mapping().nodeIds().get(0);
            siteOptions.put(currentSite, new SiteOption(currentSite, 0, gdm.node(currentSite).metrics().getCurrentCpuLoad()));

            try {
                for (IgniteReceiver receiver : f.remotes()) {
                    Fragment input = fragmentMap.get(receiver.sourceFragmentId());
                    FragmentMapping mapping = rootMapping.colocate(input.mapping()).finalize(executionNodes);

//                    UUID bestSite = null;
//                    double highestTransfer = 0;

                    for (UUID site : mapping.nodeIds()) {
                        siteOptions.putIfAbsent(site, new SiteOption(site, 1, gdm.node(site).metrics().getCurrentCpuLoad()));
                    }

//                    for (UUID site : mapping.nodeIds()) {
//                        double totalRows = 0;
//                        for (IgniteCacheTable t : fragmentCaches.get(input.fragmentId())) {
//                            long size = cacheMetrics.get(t.descriptor().cacheInfo().name()).getCacheSize(site);
//                            totalRows += size;
//                            System.out.printf("[Site %s | %s] %s rows, ", site, t.descriptor().cacheInfo().name(), size);
//                        }
//
//                        if (totalRows > highestTransfer) {
//                            bestSite = site;
//                            highestTransfer = totalRows;
//                        }
//                    }

                    // No valid remotes
//                    if (bestSite == null) continue;

                    // Record the site with the higest transfer cost for this remote
//                    if (remoteSiteSizes.getOrDefault(bestSite, 0D) < highestTransfer) {
//                        remoteSiteSizes.put(bestSite, highestTransfer);
//                        }
                }

                List<SiteOption> sorted = siteOptions
                    .values()
                    .stream()
                    .sorted()
                    .collect(Collectors.toList());

                InternalDebug.log(sorted.toString());
                UUID bestSite = sorted.get(0).nodeId;

//                double highestRowTransfer = mq.getRowCount(f.root());
//
//                for (UUID site : siteOptions) {
//                    System.out.printf("[TS %s] %s rows\n", e.getKey(), e.getValue());
//                    // TODO make this smarter with load based selection if theres multiple options
//                    if (e.getValue() > highestRowTransfer) {
//                        bestSite = e.getKey();
//                        highestRowTransfer = e.getValue();
//                    }
//                }

                if (bestSite.compareTo(currentSite) != 0) {
                    InternalDebug.log(String.format(">> [REMAPPED] fragment: %s, before: %s, best site: %s\n\n", f.fragmentId(), f.mapping().nodeIds(), bestSite));
                    fragmentMap.put(f.fragmentId(), f.remap(FragmentMapping.create(bestSite).colocate(rootMapping).finalize(executionNodes)));
                }

            } catch (ColocationMappingException ignored) {
                // There is no overlap between the input mapping and root mapping so we ignore this input
            }
        }

        List<Fragment> newFragments = new ArrayList<>(fragmentMap.values());
        try {
            FragmentMapping.create(ctx.localNodeId()).colocate(newFragments.get(0).mapping());
        } catch (ColocationMappingException e) {
            List<Fragment> frags = new FragmentSplitter(newFragments.get(0).root()).go(newFragments.get(0));
            newFragments = QueryTemplate.replace(newFragments, newFragments.get(0), Arrays.asList(frags.get(0), frags.get(1).remap(newFragments.get(0).mapping())), true);
        }

        try {
            Fragment root = newFragments.remove(0);
            FragmentMapping mapping = IgniteMdFragmentMapping._fragmentMapping(root.root(), mq, ctx);
            newFragments.add(0, root.remap(FragmentMapping.create(ctx.localNodeId()).colocate(mapping).finalize(executionNodes)));
        } catch (ColocationMappingException e) {
            // Realistically should never happen...
            throw new IgniteSQLException("Failed to optimize query.");
        }

        return new ExecutionPlan(ver, newFragments);
    }

}
