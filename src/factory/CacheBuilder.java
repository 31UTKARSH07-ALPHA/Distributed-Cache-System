package factory;

import api.ICache;
import distribution.ConsistentHashMapper;
import distribution.INodeMapper;
import events.ICacheListener;
import eviction.IEvictionPolicy;
import eviction.LRUEvictionPolicy;
import storage.ConcurrentStorage;
import storage.IStorage;
import service.LocalCache;
import service.DistributedCache;
import service.MetricsCache;

import java.util.ArrayList;
import java.util.List;

public final class CacheBuilder<K, V> {

    private int capacity = 1000;
    private long defaultTTLMillis = 0; // No TTL by default
    private boolean enableMetrics = false;
    private long ttlSweepIntervalMillis = 0; // 0 = lazy expiry only

    private IEvictionPolicy<K, V> evictionPolicy;
    private IStorage<K, V> storage;
    private INodeMapper<K> nodeMapper;
    private final List<String> nodes = new ArrayList<>();

    private CacheBuilder() {}

    public static <K, V> CacheBuilder<K, V> newBuilder() {
        return new CacheBuilder<>();
    }
    public CacheBuilder<K, V> capacity(int capacity) {
        this.capacity = capacity;
        return this;
    }
    public CacheBuilder<K, V> ttl(long ttlMillis) {
        this.defaultTTLMillis = ttlMillis;
        return this;
    }
    public CacheBuilder<K, V> addNode(String nodeAddress) {
        this.nodes.add(nodeAddress);
        return this;
    }
    public CacheBuilder<K, V> metrics(boolean enable) {
        this.enableMetrics = enable;
        return this;
    }
    // Starts a background thread per cache that removes expired entries nobody reads.
    // Lazy expiry on get() already guarantees expired values are never returned; the sweep frees memory.
    public CacheBuilder<K, V> ttlSweepInterval(long intervalMillis) {
        if (intervalMillis <= 0) throw new IllegalArgumentException("Sweep interval must be > 0");
        this.ttlSweepIntervalMillis = intervalMillis;
        return this;
    }
    public CacheBuilder<K, V> evictionPolicy(IEvictionPolicy<K, V> policy) {
        this.evictionPolicy = policy;
        return this;
    }
    public ICache<K, V> buildLocal() {
        IStorage<K, V> stor = (storage != null) ? storage : new ConcurrentStorage<>(capacity);
        IEvictionPolicy<K, V> policy = (evictionPolicy != null) ? evictionPolicy : new LRUEvictionPolicy<>(capacity);

        return wrap(withSweep(new LocalCache<>(stor, policy, defaultTTLMillis)));
    }

    // Node caches are left unwrapped: metrics are collected once, on the outer distributed cache,
    // which forwards its listeners to every node.
    private LocalCache<K, V> buildNode() {
        return withSweep(new LocalCache<>(new ConcurrentStorage<>(capacity), new LRUEvictionPolicy<>(capacity), defaultTTLMillis));
    }

    private LocalCache<K, V> withSweep(LocalCache<K, V> cache) {
        if (ttlSweepIntervalMillis > 0) cache.startTTLSweep(ttlSweepIntervalMillis);
        return cache;
    }

    public ICache<K, V> buildDistributed() {
        if (nodes.isEmpty()) throw new IllegalStateException("Nodes required for distributed mode.");
        // A policy instance holds per-cache state (the LRU list); sharing one across nodes would corrupt it.
        if (evictionPolicy != null) {
            throw new IllegalStateException(
                    "Custom evictionPolicy instances can't be shared across nodes; distributed mode uses LRU per node.");
        }
        INodeMapper<K> mapper = (nodeMapper != null) ? nodeMapper : new ConsistentHashMapper<>();
        nodes.forEach(mapper::addNode);
        ICache<K, V> distributed = new DistributedCache<>(mapper, addr -> buildNode());
        return wrap(distributed);
    }

    private ICache<K, V> wrap(ICache<K, V> cache) {
        return enableMetrics ? new MetricsCache<>(cache) : cache;
    }
}