package service;


import events.EvictionCause;
import eviction.IEvictionPolicy;
import storage.CacheEntry;
import storage.IStorage;

import java.util.Optional;

public class LocalCache<K, V> extends AbstractBaseCache<K, V> {

    private final IEvictionPolicy<K, V> evictionPolicy;
    private final long defaultTTLMillis;

    public LocalCache(IStorage<K, V> storage,
                      IEvictionPolicy<K, V> evictionPolicy,
                      long defaultTTLMillis) {
        super(storage);
        this.evictionPolicy  = evictionPolicy;
        this.defaultTTLMillis = defaultTTLMillis;
    }
    @Override
    protected void onHitHook(K key, CacheEntry<K, V> entry) {
        evictionPolicy.onAccess(key);
    }

    @Override
    protected void onInsertHook(K key, CacheEntry<K, V> entry) {
        evictionPolicy.onInsert(key);
    }

    @Override
    protected void onRemoveHook(K key) {
        evictionPolicy.onRemove(key);
    }

    // Called from put() while holding the cache lock. evict() already drops the key from the policy.
    @Override
    protected void evictIfNeeded() {
        Optional<K> victim = evictionPolicy.evict();
        victim.ifPresent(key ->
                storage.remove(key).ifPresent(entry ->
                        fireOnEviction(key, entry.getValue(), EvictionCause.CAPACITY))
        );
    }

    @Override
    protected long defaultTTLMillis() {
        return defaultTTLMillis;
    }

    public void startTTLSweep(long intervalMillis) {
        ttlManager.startScheduledSweep(intervalMillis);
    }
}
