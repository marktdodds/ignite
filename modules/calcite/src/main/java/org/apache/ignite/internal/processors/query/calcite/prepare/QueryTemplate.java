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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.PartitionExtractor;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

/** */
public class QueryTemplate {
    /** */
    private final ImmutableList<Fragment> fragments;

    /** */
    private final AtomicReference<ExecutionPlan> executionPlan = new AtomicReference<>();

    /** */
    public QueryTemplate(List<Fragment> fragments) {
        ImmutableList.Builder<Fragment> b = ImmutableList.builder();
        for (Fragment fragment : fragments)
            b.add(fragment.attach(Commons.emptyCluster()));

        this.fragments = b.build();
    }

    /** */
    public ExecutionPlan map(MappingService mappingService, MappingQueryContext ctx) {
        ExecutionPlan executionPlan = this.executionPlan.get();

        if (executionPlan != null && Objects.equals(executionPlan.topologyVersion(), ctx.topologyVersion()))
            return executionPlan;

        List<Fragment> fragments = Commons.transform(this.fragments, f -> f.attach(ctx.cluster()));

        Exception ex = null;
        RelMetadataQuery mq = F.first(fragments).root().getCluster().getMetadataQuery();

        for (int i = 0; i < 3; i++) {
            try {
                fragments = map(mappingService, fragments, ctx, mq);

                List<PartitionNode> partNodes = Commons.transform(fragments, f -> new PartitionExtractor(ctx.typeFactory()).go(f));

                ExecutionPlan executionPlan0 = new ExecutionPlan(ctx.topologyVersion(), fragments, partNodes);

                if (executionPlan == null || executionPlan.topologyVersion().before(executionPlan0.topologyVersion()))
                    this.executionPlan.compareAndSet(executionPlan, executionPlan0);

                return executionPlan0;
            }
            catch (FragmentMappingException e) {
                if (ex == null)
                    ex = e;
                else
                    ex.addSuppressed(e);

                fragments = replace(fragments, e.fragment(), new FragmentSplitter(e.node()).go(e.fragment()));
            }
        }

        throw new IgniteSQLException("Failed to map query.", ex);
    }

    /** */
    @NotNull private List<Fragment> map(
        MappingService mappingService,
        List<Fragment> fragments,
        MappingQueryContext ctx,
        RelMetadataQuery mq
    ) {
        ImmutableList.Builder<Fragment> b = ImmutableList.builder();

        for (Fragment fragment : fragments)
            b.add(fragment.map(mappingService, ctx, mq).attach(Commons.emptyCluster()));

        return b.build();
    }

    /** */
    protected static List<Fragment> replace(List<Fragment> fragments, Fragment fragment, List<Fragment> replacement) {
        return replace(fragments, fragment, replacement, false);
    }

    protected static List<Fragment> replace(List<Fragment> fragments, Fragment fragmentToReplace, List<Fragment> replacements, boolean preserveMapping) {
        assert !F.isEmpty(replacements);

        Map<Long, Long> newTargets = new HashMap<>();

        for (Fragment fragment0 : replacements) {
            for (IgniteReceiver remote : fragment0.remotes())
                newTargets.put(remote.exchangeId(), fragment0.fragmentId());
        }

        List<Fragment> newFragments = new ArrayList<>(fragments.size() + replacements.size() - 1);

        for (Fragment frag : fragments) {
            if (frag == fragmentToReplace) {
//                frag = F.first(replacement);
                newFragments.addAll(replacements);
                continue;
            }
            else if (!frag.rootFragment()) {
                IgniteSender sender = (IgniteSender)frag.root();
                Long newTargetId = newTargets.get(sender.exchangeId());

                if (newTargetId != null && newTargetId != sender.targetFragmentId()) {
                    sender = new IgniteSender(sender.getCluster(), sender.getTraitSet(),
                        sender.getInput(), sender.exchangeId(), newTargetId, sender.distribution());

                    frag = preserveMapping ? new Fragment(frag.fragmentId(), sender, frag.remotes(), null, frag.mapping())
                        : new Fragment(frag.fragmentId(), sender, frag.remotes());
                }
            }

            newFragments.add(frag);
        }

//        newFragments.addAll(replacements.subList(1, replacements.size()));

        return newFragments;
    }
}
