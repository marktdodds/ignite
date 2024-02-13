package org.apache.ignite.internal.processors.query.calcite.exec.cache;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResultCache {

    /**  */
    public static final ResultCache CACHE = new ResultCache();

    /**  */
    private static final long MAX_CACHE_SIZE = IgniteSystemProperties.getLong("MD_MAX_CACHE_SIZE_MB", 1024) * 1000000;

    /**  */
    private long currentCacheSize;

    /** Cache lock */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Rel Cache */
    private final ArrayList<CacheEntry> cachedRels = new ArrayList<>();

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

}
