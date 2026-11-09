package service;

import api.ICache;
import api.ICacheEvents;
import api.ICacheMetrics;
import events.EvictionCause;
import events.NoOpCacheListener;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public class MetricsCache<K, V> implements ICache<K, V>, ICacheMetrics {

    private final ICache<K, V> delegate;

    private final LongAdder hits      = new LongAdder();
    private final LongAdder misses    = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final LongAdder expiries  = new LongAdder();

    public MetricsCache(ICache<K, V> delegate) {
        this.delegate = delegate;
        // Evictions and expiries happen inside the delegate, not in a call that passes through here,
        // so subscribe to its events (Observer) to count them.
        if (delegate instanceof ICacheEvents<?, ?>) {
            @SuppressWarnings("unchecked")
            ICacheEvents<K, V> events = (ICacheEvents<K, V>) delegate;
            events.addListener(new NoOpCacheListener<>() {
                @Override
                public void onEviction(K key, V value, EvictionCause cause) {
                    // Only capacity pressure counts; explicit invalidate() is not an eviction.
                    if (cause == EvictionCause.CAPACITY) evictions.increment();
                }
                @Override
                public void onExpiry(K key, V value) {
                    expiries.increment();
                }
            });
        }
    }
    @Override
    public Optional<V> get(K key) {
        Optional<V> result = delegate.get(key);
        if (result.isPresent()) hits.increment();
        else                    misses.increment();
        return result;
    }
    @Override
    public void put(K key, V value) { delegate.put(key, value); }
    @Override
    public void put(K key, V value, long ttlMillis) { delegate.put(key, value, ttlMillis); }
    @Override
    public void invalidate(K key) { delegate.invalidate(key); }
    @Override
    public void invalidateAll()  { delegate.invalidateAll(); }
    @Override
    public long size() { return delegate.size(); }
    @Override
    public boolean containsKey(K key) { return delegate.containsKey(key); }

    @Override
    public double hitRate() {
        long total = totalRequests();
        return total == 0 ? 0.0 : (double) hits.sum() / total;
    }

    @Override
    public double missRate() {
        return 1.0 - hitRate();
    }
    @Override
    public long totalRequests()  { return hits.sum() + misses.sum(); }
    @Override
    public long totalHits()      { return hits.sum(); }
    @Override
    public long totalMisses()    { return misses.sum(); }
    @Override
    public long totalEvictions() { return evictions.sum(); }
    @Override
    public long totalExpiries()  { return expiries.sum(); }

    @Override
    public void resetMetrics() {
        hits.reset();
        misses.reset();
        evictions.reset();
        expiries.reset();
    }
    public void recordEviction() { evictions.increment(); }
    public void recordExpiry()   { expiries.increment(); }

    @Override
    public String toString() {
        return String.format(
                "MetricsCache{hits=%d, misses=%d, hitRate=%.2f%%, evictions=%d, expiries=%d, size=%d}",
                totalHits(), totalMisses(), hitRate() * 100, totalEvictions(), totalExpiries(), size()
        );
    }
}
