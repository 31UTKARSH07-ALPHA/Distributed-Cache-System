package factory;

import api.ICache;
import eviction.LRUEvictionPolicy;

public final class CacheFactory {

    private CacheFactory() {}

    public static <K, V> ICache<K, V> lruLocal(int capacity, long defaultTTLMillis) {
        CacheBuilder<K, V> builder = CacheBuilder.<K, V>newBuilder()
                .capacity(capacity)
                .ttl(defaultTTLMillis)
                .evictionPolicy(new LRUEvictionPolicy<>(capacity))
                .metrics(true);
        // Sweep once per TTL period; with no TTL nothing expires, so no sweep thread is needed.
        if (defaultTTLMillis > 0) builder.ttlSweepInterval(defaultTTLMillis);
        return builder.buildLocal();
    }
    public static <K, V> ICache<K, V> distributed(int nodeCapacity, String... nodeAddresses) {
        CacheBuilder<K, V> builder = CacheBuilder.<K, V>newBuilder()
                .capacity(nodeCapacity)
                .metrics(true);

        for (String node : nodeAddresses) {
            builder.addNode(node);
        }
        return builder.buildDistributed();
    }
    public static <K, V> ICache<K, V> unbounded() {
        return CacheBuilder.<K, V>newBuilder()
                .capacity(Integer.MAX_VALUE)
                .buildLocal();
    }
}