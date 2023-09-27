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
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

/**
 * Splits a query into a list of query fragments.
 */
public class Splitter extends IgniteRelShuttle {
    /** */
    private final Deque<FragmentProto> stack = new LinkedList<>();

    /** */
    private FragmentProto curr;

    /** */
    public List<Fragment> go(IgniteRel root) {
        ArrayList<Fragment> res = new ArrayList<>();

        stack.push(new FragmentProto(IdGenerator.nextId(), root));

        while (!stack.isEmpty()) {
            curr = stack.pop();

            curr.root = visit(curr.root);

            res.add(curr.build());

            curr = null;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        RelOptCluster cluster = rel.getCluster();

        long targetFragmentId = curr.id;
        long sourceFragmentId = IdGenerator.nextId();
        long exchangeId = sourceFragmentId;

        IgniteSender sender = new IgniteSender(cluster, rel.getTraitSet(), rel.getInput(), exchangeId, targetFragmentId,
            rel.distribution());
        IgniteReceiver receiver = new IgniteReceiver(cluster, rel.getTraitSet(), rel.getRowType(), exchangeId, sourceFragmentId, sender);

        curr.remotes.add(receiver);
        stack.push(new FragmentProto(sourceFragmentId, sender));

        return receiver;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTrimExchange rel) {
        return ((IgniteTrimExchange)processNode(rel)).clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        return rel.clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return rel.clone(IdGenerator.nextId());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteNestedLoopJoin rel) {
        return forceExchangesForJoins(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMergeJoin rel) {
        return forceExchangesForJoins(rel);
    }

    /**
     * Replaces a non-exchange inputs with exchanges so the site-selection optimizer can work.
     * Does not work with a NestedCorrelatedLoopJoin because of the context() sharing between the join node
     * and the input nodes for correlation ids
     */
    public IgniteRel forceExchangesForJoins(AbstractIgniteJoin rel) {
        for (int i = 0; i < rel.getInputs().size(); i++) {
            RelNode input = rel.getInput(i);
            if (!(input instanceof IgniteExchange)) {
                IgniteExchange exchange = new IgniteExchange(input.getCluster(), input.getTraitSet(), input, TraitUtils.distribution(input));
                rel.replaceInput(i, exchange);
            }
        }

        return processNode(rel);
    }

    /** */
    private static class FragmentProto {
        /** */
        private final long id;

        /** */
        private IgniteRel root;

        /** */
        private final ImmutableList.Builder<IgniteReceiver> remotes = ImmutableList.builder();

        /** */
        private FragmentProto(long id, IgniteRel root) {
            this.id = id;
            this.root = root;
        }

        /** */
        Fragment build() {
            return new Fragment(id, root, remotes.build());
        }
    }
}
