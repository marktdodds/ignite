package org.apache.ignite.internal.processors.query.calcite.exec.cache;

import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.cache.CacheableIgniteDistributedHashJoin;

public class CachedRelInjector extends IgniteRelShuttle {

    final ResultCache cache;

    public CachedRelInjector(ResultCache cache) {
        this.cache = cache;
    }

    @Override
    public IgniteRel visit(CacheableIgniteDistributedHashJoin rel) {
        CacheableIgniteDistributedHashJoin cached = cache.lookupInCache(rel);
        if (cached != null) rel.setCachedExecutionNode(cached.getCachedExecutionNode());

        return processNode(rel);
    }
}
