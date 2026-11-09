package service;

import api.ICache;
import api.ICacheEvents;
import events.EvictionCause;
import events.ICacheListener;
import storage.CacheEntry;
import storage.IStorage;
import ttl.TTLManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractBaseCache<K, V>
        implements ICache<K, V>, ICacheEvents<K, V> {
    private static final Logger log = Logger.getLogger(AbstractBaseCache.class.getName());
    protected final IStorage<K, V> storage;
    protected final TTLManager<K, V> ttlManager;
    protected final List<ICacheListener<K, V>> listeners = new CopyOnWriteArrayList<>();

    // One lock per cache instance. get() also mutates state (the eviction policy's access order),
    // so reads and writes both take it; a read-write lock would not help here. In DistributedCache
    // every node is its own instance, so threads hitting different nodes never contend (lock striping).
    protected final ReentrantLock lock = new ReentrantLock();

    protected AbstractBaseCache(IStorage<K, V> storage) {
        this.storage    = storage;
        this.ttlManager = new TTLManager<>(storage, this::expireIfStale);
    }
    @Override
    public final Optional<V> get(K key) {
        lock.lock();
        try {
            Optional<CacheEntry<K, V>> entry = storage.get(key);
            if (entry.isPresent() && entry.get().isExpired()) {
                expireIfStale(key);
                fireOnMiss(key);
                return Optional.empty();
            }
            if (entry.isEmpty()) {
                onMissHook(key);
                fireOnMiss(key);
                return Optional.empty();
            }
            onHitHook(key, entry.get());
            fireOnHit(key, entry.get().getValue());
            return Optional.of(entry.get().getValue());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final void put(K key, V value) {
        put(key, value, defaultTTLMillis());
    }

    @Override
    public final void put(K key, V value, long ttlMillis) {
        CacheEntry<K, V> entry = (ttlMillis <= 0)
                ? CacheEntry.withoutTTL(key, value)
                : CacheEntry.withTTL(key, value, ttlMillis);

        lock.lock();
        try {
            storage.put(key, entry);
            onInsertHook(key, entry);
            fireOnPut(key, value);
            evictIfNeeded();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public final void invalidate(K key) {
        lock.lock();
        try {
            storage.remove(key).ifPresent(entry -> {
                onRemoveHook(key);
                fireOnEviction(key, entry.getValue(), EvictionCause.EXPLICIT);
            });
        } finally {
            lock.unlock();
        }
    }
    @Override
    public final void invalidateAll() {
        storage.keys().forEach(this::invalidate);
    }
    @Override
    public final long size() {
        return storage.size();
    }
    @Override
    public final boolean containsKey(K key) {
        lock.lock();
        try {
            Optional<CacheEntry<K, V>> entry = storage.get(key);
            if (entry.isPresent() && entry.get().isExpired()) {
                expireIfStale(key);
                return false;
            }
            return entry.isPresent();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public void addListener(ICacheListener<K, V> listener) {
        ((CopyOnWriteArrayList<ICacheListener<K, V>>) listeners).addIfAbsent(listener);
    }
    @Override
    public void removeListener(ICacheListener<K, V> listener) {
        listeners.remove(listener);
    }

    // Single removal path for expired entries: lazy expiry (get/containsKey) and the background
    // sweep thread both land here, under the same lock. Re-checks expiry because the entry may have
    // been overwritten with a fresh value between the sweep's scan and this call.
    private void expireIfStale(K key) {
        lock.lock();
        try {
            storage.get(key)
                    .filter(CacheEntry::isExpired)
                    .ifPresent(entry -> {
                        storage.remove(key);
                        onRemoveHook(key);   // drop it from the eviction policy too, or it lingers as a ghost
                        fireOnExpiry(key, entry.getValue());
                    });
        } finally {
            lock.unlock();
        }
    }

    // --- Rebalancing support (used by DistributedCache when nodes join or leave) ---

    // Live (non-expired) entries at this moment.
    List<CacheEntry<K, V>> liveEntries() {
        lock.lock();
        try {
            List<CacheEntry<K, V>> live = new ArrayList<>();
            for (CacheEntry<K, V> entry : storage.entries()) {
                if (!entry.isExpired()) live.add(entry);
            }
            return live;
        } finally {
            lock.unlock();
        }
    }

    // Accepts a migrated entry unless a newer write for the key already landed here. The entry object
    // is stored as-is, so its original expiry time carries over. No put/eviction events: it's a move.
    void acceptMigrated(CacheEntry<K, V> entry) {
        lock.lock();
        try {
            K key = entry.getKey();
            boolean hasNewerValue = storage.get(key).filter(existing -> !existing.isExpired()).isPresent();
            if (hasNewerValue || entry.isExpired()) return;
            storage.put(key, entry);
            onInsertHook(key, entry);
            evictIfNeeded();
        } finally {
            lock.unlock();
        }
    }

    // Drops a key that has moved to another node, without firing eviction events.
    void releaseMigrated(K key) {
        lock.lock();
        try {
            storage.remove(key).ifPresent(entry -> onRemoveHook(key));
        } finally {
            lock.unlock();
        }
    }

    protected void onHitHook(K key, CacheEntry<K, V> entry) {}
    protected void onMissHook(K key) {}
    protected void onInsertHook(K key, CacheEntry<K, V> entry) {}
    protected void onRemoveHook(K key) {}
    protected void evictIfNeeded() {}
    protected long defaultTTLMillis() { return 0L; }
    private void fireOnHit(K key, V value) {
        for (ICacheListener<K, V> l : listeners) safeCall(() -> l.onHit(key, value));
    }

    private void fireOnMiss(K key) {
        for (ICacheListener<K, V> l : listeners) safeCall(() -> l.onMiss(key));
    }
    private void fireOnPut(K key, V value) {
        for (ICacheListener<K, V> l : listeners) safeCall(() -> l.onPut(key, value));
    }
    protected void fireOnEviction(K key, V value, EvictionCause cause) {
        for (ICacheListener<K, V> l : listeners) safeCall(() -> l.onEviction(key, value, cause));
    }
    private void fireOnExpiry(K key, V value) {
        for (ICacheListener<K, V> l : listeners) safeCall(() -> l.onExpiry(key, value));
    }
    private void safeCall(Runnable r) {
        try { r.run(); } catch (Exception e) {
            log.log(Level.WARNING, "Cache listener threw an exception", e);
        }
    }
}
