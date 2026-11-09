package service;

import api.ICache;
import api.ICacheEvents;
import distribution.INodeMapper;
import events.ICacheListener;
import storage.CacheEntry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DistributedCache <K,V> implements ICache<K,V>, ICacheEvents<K,V> {
    private final INodeMapper<K> nodeMapper;

    private final Map<String, AbstractBaseCache<K, V>> nodeCaches;
    // Kept so nodes added later get the same listeners as existing ones.
    private final List<ICacheListener<K, V>> listeners = new CopyOnWriteArrayList<>();
    public interface NodeCacheFactory<K, V> {
        AbstractBaseCache<K, V> create(String nodeAddress);
    }

    public DistributedCache(INodeMapper<K> nodeMapper,
                            NodeCacheFactory<K, V> factory) {
        this.nodeMapper  = nodeMapper;
        this.nodeCaches  = new ConcurrentHashMap<>();
        nodeMapper.allNodes().forEach(node ->
                nodeCaches.put(node, factory.create(node))
        );
    }
    @Override
    public Optional<V> get(K key) {
        return cacheFor(key).get(key);
    }

    @Override
    public void put(K key, V value) {
        cacheFor(key).put(key, value);
    }

    @Override
    public void put(K key, V value, long ttlMillis) {
        cacheFor(key).put(key, value, ttlMillis);
    }

    @Override
    public void invalidate(K key) {
        cacheFor(key).invalidate(key);
    }
    @Override
    public void invalidateAll() {
        nodeCaches.values().forEach(ICache::invalidateAll);
    }
    @Override
    public long size() {
        return nodeCaches.values().stream().mapToLong(ICache::size).sum();
    }
    @Override
    public boolean containsKey(K key) {
        return cacheFor(key).containsKey(key);
    }

    // Rebalancing: when a node joins, the keys it now owns (~1/N of them) are moved to it from the
    // other nodes. Without this, those keys would be misses, and their old copies would resurface as
    // STALE values if the node later left and the keys mapped back. Order matters: the new node is
    // registered before routing switches to it, and moves never overwrite a newer write.
    public synchronized void addNode(String nodeAddress, NodeCacheFactory<K, V> factory) {
        if (nodeCaches.containsKey(nodeAddress)) {
            throw new IllegalArgumentException("Node already present: " + nodeAddress);
        }
        AbstractBaseCache<K, V> newCache = factory.create(nodeAddress);
        listeners.forEach(newCache::addListener);
        nodeCaches.put(nodeAddress, newCache);
        nodeMapper.addNode(nodeAddress);

        for (Map.Entry<String, AbstractBaseCache<K, V>> node : nodeCaches.entrySet()) {
            if (node.getKey().equals(nodeAddress)) continue;
            AbstractBaseCache<K, V> oldCache = node.getValue();
            for (CacheEntry<K, V> entry : oldCache.liveEntries()) {
                if (nodeMapper.getNode(entry.getKey()).equals(nodeAddress)) {
                    newCache.acceptMigrated(entry);
                    oldCache.releaseMigrated(entry.getKey());
                }
            }
        }
    }

    // When a node leaves, its live entries move to whichever nodes now own them.
    public synchronized void removeNode(String nodeAddress) {
        if (!nodeCaches.containsKey(nodeAddress)) return;
        if (nodeCaches.size() == 1) {
            throw new IllegalStateException("Cannot remove the last node: " + nodeAddress);
        }
        nodeMapper.removeNode(nodeAddress);
        AbstractBaseCache<K, V> removed = nodeCaches.remove(nodeAddress);
        for (CacheEntry<K, V> entry : removed.liveEntries()) {
            cacheFor(entry.getKey()).acceptMigrated(entry);
        }
    }

    @Override
    public void addListener(ICacheListener<K, V> listener) {
        if (!((CopyOnWriteArrayList<ICacheListener<K, V>>) listeners).addIfAbsent(listener)) return;
        nodeCaches.values().forEach(cache -> cache.addListener(listener));
    }

    @Override
    public void removeListener(ICacheListener<K, V> listener) {
        listeners.remove(listener);
        nodeCaches.values().forEach(cache -> cache.removeListener(listener));
    }

    private AbstractBaseCache<K, V> cacheFor(K key) {
        String node = nodeMapper.getNode(key);
        AbstractBaseCache<K, V> cache = nodeCaches.get(node);
        if (cache == null) {
            throw new IllegalStateException("No cache found for node: " + node);
        }
        return cache;
    }
}
