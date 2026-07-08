import api.ICache;
import api.ICacheMetrics;
import factory.CacheBuilder;
import factory.CacheFactory;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("  Distributed Cache — SOLID + Design Patterns Demo");
        demoLocalLRU();
        demoTTL();
        demoDistributed();
        demoFactoryPresets();
        System.out.println("  All demos complete.");
    }
    private static void demoLocalLRU() {
        System.out.println("--- 1. LRU Local Cache ---");

        ICache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .capacity(3)
                .metrics(true)
                .buildLocal();

        cache.put("a", "Apple");
        cache.put("b", "Banana");
        cache.put("c", "Cherry");

        cache.get("a");
        cache.put("d", "Date");

        System.out.println("  get(a) = " + cache.get("a").orElse("MISS"));
        System.out.println("  get(b) = " + cache.get("b").orElse("MISS (evicted by LRU — expected)"));
        System.out.println("  get(d) = " + cache.get("d").orElse("MISS"));
        printMetrics(cache);
        System.out.println();
    }
    private static void demoTTL() throws InterruptedException {
        System.out.println("--- 2. Cache with TTL Expiry ---");

        ICache<String, Integer> cache = CacheBuilder.<String, Integer>newBuilder()
                .capacity(100)
                .ttl(200)
                .metrics(true)
                .buildLocal();

        cache.put("session:1", 42);
        System.out.println("  Before expiry: " + cache.get("session:1").orElse(-1));
        Thread.sleep(300);
        System.out.println("  After expiry:  " + cache.get("session:1").orElse(-1) + " (MISS — expired)");
        printMetrics(cache);
        System.out.println();
    }
    private static void demoDistributed() {
        System.out.println("--- 3. Distributed Cache — Consistent Hashing ---");
        ICache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .capacity(500)
                .addNode("node1:6379")
                .addNode("node2:6379")
                .addNode("node3:6379")
                .metrics(true)
                .buildDistributed();

        String[] keys = {"user:1", "user:2", "product:A", "product:B", "order:XYZ"};
        for (String key : keys) {
            cache.put(key, "value-of-" + key);
        }

        for (String key : keys) {
            System.out.println("  get(" + key + ") = " + cache.get(key).orElse("MISS"));
        }

        printMetrics(cache);
        System.out.println();
    }
    private static void demoFactoryPresets() {
        System.out.println("--- 4. CacheFactory Presets ---");

        ICache<String, String> lru = CacheFactory.lruLocal(1_000, 60_000);
        lru.put("k", "v");
        System.out.println("  LRU preset get: " + lru.get("k").orElse("MISS"));
        ICache<String, String> dist = CacheFactory.distributed(500, "node1", "node2");
        dist.put("product:42", "Widget");
        System.out.println("  Distributed preset get: " + dist.get("product:42").orElse("MISS"));

        System.out.println();
    }
    private static void printMetrics(ICache<?, ?> cache) {
        // Cleaned up double-printing logic
        if (cache instanceof ICacheMetrics m) {
            System.out.printf("  Metrics → hits=%d  misses=%d  hitRate=%.0f%%  size=%d%n",
                    m.totalHits(), m.totalMisses(), m.hitRate() * 100, cache.size());
        }
    }
}