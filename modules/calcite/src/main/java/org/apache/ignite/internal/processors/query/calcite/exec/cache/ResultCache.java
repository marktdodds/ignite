package org.apache.ignite.internal.processors.query.calcite.exec.cache;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.schema.AbstractSchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.util.InternalDebug;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResultCache extends AbstractService {

    /**  */
    private final GridInternalSubscriptionProcessor subscriptionProc;

    /**  */
    private static final long MAX_CACHE_SIZE = IgniteSystemProperties.getLong("MD_MAX_CACHE_SIZE_MB", 1024) * 1000000;

    /**  */
    private final GridEventStorageManager events;

    /**  */
    private long currentCacheSize;

    /** Cache lock */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Rel Cache */
    private ArrayList<CacheEntry> cachedRels = new ArrayList<>();

    /**
     * @param ctx Kernal context.
     */
    public ResultCache(GridKernalContext ctx) {
        super(ctx);

        subscriptionProc = ctx.internalSubscriptionProcessor();
        events = ctx.event();

        init();
    }

    /** {@inheritDoc} */
    @Override
    public void init() {
        subscriptionProc.registerSchemaChangeListener(new ResultCache.SchemaListener());

        // We only clear on FAILED or LEFT, a node joining does not change the data only its layout
        events.addDiscoveryEventListener((evt, discoCache) -> clear(), EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /**
     * Lookup an IgniteRel in the cache, returning the match if found (or null if not)
     */
    @Nullable <T extends IgniteRel> T lookupInCache(T lookup) {
        lock.readLock().lock();
        try {
            for (CacheEntry cached : cachedRels) {
                if (new CachedRelMatcher(lookup).visit(cached.rel)) {
                    cached.cacheHit();
                    return (T) cached.rel;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Adds a IgniteRel that has completed its execution to the cache. If it already exists in the cache (possible
     * if the same execution plan is executed for the first time twice from 2 different requests) it is skipped.
     */
    public void checkAndAddCompletedRel(IgniteRel executionRoot, long size) {
        if (size <= 0) return;
        lock.writeLock().lock();
        try {
            if (lookupInCache(executionRoot) != null) return;
            if (size + currentCacheSize > MAX_CACHE_SIZE && !tryEvict()) return;

            currentCacheSize += size;
            cachedRels.add(new CacheEntry(executionRoot, size));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Tries to evict something from the cache if possible. {lock.writeLock()} must be held before entering this method
     *
     * @return bool indicating eviction success
     */
    private boolean tryEvict() {
        if (cachedRels.isEmpty()) return false;
        return evictLRU();
    }

    /**
     * LRU eviction
     */
    private boolean evictLRU() {
        assert !cachedRels.isEmpty();
        int index = 0;
        long lastUsed = cachedRels.get(0).lastUsed;
        for (int i = 1; i < cachedRels.size(); i++) {
            if (cachedRels.get(i).lastUsed < lastUsed) {
                lastUsed = cachedRels.get(i).lastUsed;
                index = i;
            }
        }
        CacheEntry e = cachedRels.remove(index);
        currentCacheSize -= e.size;
        return true;
    }

    private void clear() {
        lock.writeLock().lock();
        try {
            cachedRels = new ArrayList<>();
            InternalDebug.alwaysLog("Clearing result cache!");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * CacheEntry wrapper class
     */
    private static class CacheEntry {
        private final IgniteRel rel;
        private final long size;
        private long lastUsed;

        private CacheEntry(IgniteRel rel, long size) {
            this.rel = rel;
            this.size = size;
        }

        public void cacheHit() {
            lastUsed = System.currentTimeMillis();
        }
    }

    /** Schema change listener. */
    private class SchemaListener extends AbstractSchemaChangeListener {
        /** {@inheritDoc} */
        @Override
        public void onSchemaDropped(String schemaName) {
            clear();
        }

        /** {@inheritDoc} */
        @Override
        public void onSqlTypeDropped(
            String schemaName,
            GridQueryTypeDescriptor typeDescriptor,
            boolean destroy
        ) {
            clear();
        }

        /** {@inheritDoc} */
        @Override
        public void onIndexCreated(
            String schemaName,
            String tblName,
            String idxName,
            IndexDescriptor idxDesc
        ) {
            clear();
        }

        /** {@inheritDoc} */
        @Override
        public void onIndexDropped(String schemaName, String tblName, String idxName) {
            clear();
        }

        /** {@inheritDoc} */
        @Override
        public void onIndexRebuildStarted(String schemaName, String tblName) {
            clear();
        }

        /** {@inheritDoc} */
        @Override
        public void onIndexRebuildFinished(String schemaName, String tblName) {
            clear();
        }

        /** {@inheritDoc} */
        @Override
        public void onColumnsAdded(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            GridCacheContextInfo<?, ?> cacheInfo,
            List<QueryField> cols
        ) {
            clear();
        }

        /** {@inheritDoc} */
        @Override
        public void onColumnsDropped(
            String schemaName,
            GridQueryTypeDescriptor typeDesc,
            GridCacheContextInfo<?, ?> cacheInfo,
            List<String> cols
        ) {
            clear();
        }
    }

}
