package distribution;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashMapper<K> implements INodeMapper<K> {

    private static final int DEFAULT_VIRTUAL_NODES = 150;
    private final int virtualNodes;
    private final ConcurrentSkipListMap<Long, String> ring = new ConcurrentSkipListMap<>();
    private final Map<String, List<Long>> nodePositions = new HashMap<>();
    public ConsistentHashMapper(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }
    public ConsistentHashMapper() {
        this(DEFAULT_VIRTUAL_NODES);
    }
    @Override
    public String getNode(K key) {
        if (ring.isEmpty()) throw new IllegalStateException("No nodes registered");
        long hash = hash(key.toString());
        Map.Entry<Long, String> entry = ring.ceilingEntry(hash);
        return entry != null ? entry.getValue() : ring.firstEntry().getValue();
    }
    @Override
    public List<String> getNodes(K key, int replicationFactor) {
        if (ring.isEmpty()) throw new IllegalStateException("No nodes registered");
        long hash = hash(key.toString());
        List<String> result = new ArrayList<>();
        Set<String> seen    = new LinkedHashSet<>();

        NavigableMap<Long, String> tail = ring.tailMap(hash, true);
        for (String node : tail.values()) {
            if (seen.add(node)) result.add(node);
            if (result.size() >= replicationFactor) return result;
        }
        for (String node : ring.values()) {
            if (seen.add(node)) result.add(node);
            if (result.size() >= replicationFactor) return result;
        }
        return result;
    }

    @Override
    public synchronized void addNode(String node) {
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < virtualNodes; i++) {
            long pos = hash(node + "#" + i);
            ring.put(pos, node);
            positions.add(pos);
        }
        nodePositions.put(node, positions);
    }
    @Override
    public synchronized void removeNode(String node) {
        List<Long> positions = nodePositions.remove(node);
        if (positions != null) positions.forEach(ring::remove);
    }
    @Override
    public synchronized List<String> allNodes() {
        return List.copyOf(nodePositions.keySet());
    }
    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 unavailable", e);
        }
    }
}
