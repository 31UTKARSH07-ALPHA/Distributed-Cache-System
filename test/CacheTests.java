import api.ICache;
import api.ICacheMetrics;
import distribution.ConsistentHashMapper;
import factory.CacheBuilder;
import service.DistributedCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// Dependency-free test runner (matches the project's zero-dependency design).
// Run: javac -d out $(find src test -name '*.java') && java -cp out CacheTests
public class CacheTests {

    private static int passed = 0;
    private static final List<String> failures = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        test("LRU evicts the least recently used key", CacheTests::lruEvictsLeastRecentlyUsed);
        test("Concurrent get/put never exceeds capacity or throws", CacheTests::concurrentAccessRespectsCapacity);
        test("Metrics count capacity evictions and expiries", CacheTests::metricsCountEvictionsAndExpiries);
        test("Distributed metrics count evictions across nodes", CacheTests::distributedMetricsCountEvictions);
        test("Adding a node loses no keys", CacheTests::addNodeMigratesKeys);
        test("Add then remove a node never returns stale values", CacheTests::addThenRemoveNodeHasNoStaleReads);
        test("Migrated entries keep their original TTL", CacheTests::migrationPreservesTtl);
        test("Background sweep removes expired keys nobody reads", CacheTests::sweepRemovesUnreadExpiredKeys);
        test("Distributed build rejects a shared eviction policy", CacheTests::distributedRejectsSharedPolicy);

        System.out.printf("%n%d passed, %d failed%n", passed, failures.size());
        failures.forEach(f -> System.out.println("  FAILED: " + f));
        if (!failures.isEmpty()) System.exit(1);
    }

    static void lruEvictsLeastRecentlyUsed() {
        ICache<String, String> cache = CacheBuilder.<String, String>newBuilder().capacity(3).buildLocal();
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        cache.get("a");          // a is now most recently used, so b is the LRU
        cache.put("d", "4");

        check(cache.get("a").isPresent(), "a should survive (recently used)");
        check(cache.get("b").isEmpty(), "b should be evicted (least recently used)");
        check(cache.size() == 3, "size should stay at capacity 3, was " + cache.size());
    }

    static void concurrentAccessRespectsCapacity() throws Exception {
        int capacity = 1_000;
        ICache<Integer, Integer> cache = CacheBuilder.<Integer, Integer>newBuilder().capacity(capacity).buildLocal();
        AtomicInteger errors = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < 8; t++) {
            futures.add(pool.submit(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                for (int i = 0; i < 200_000; i++) {
                    int key = random.nextInt(5_000);
                    try {
                        if (random.nextBoolean()) cache.put(key, key);
                        else cache.get(key);
                    } catch (Throwable e) {
                        errors.incrementAndGet();
                    }
                }
            }));
        }
        for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        check(errors.get() == 0, "expected no exceptions, got " + errors.get());
        check(cache.size() <= capacity, "size " + cache.size() + " exceeded capacity " + capacity);
    }

    static void metricsCountEvictionsAndExpiries() throws Exception {
        ICache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .capacity(2).ttl(50).metrics(true).buildLocal();
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");     // evicts a
        cache.put("d", "4");     // evicts b
        Thread.sleep(100);
        cache.get("c");          // expired
        cache.get("d");          // expired
        cache.invalidate("x");   // explicit removal is not an eviction

        ICacheMetrics metrics = (ICacheMetrics) cache;
        check(metrics.totalEvictions() == 2, "expected 2 evictions, got " + metrics.totalEvictions());
        check(metrics.totalExpiries() == 2, "expected 2 expiries, got " + metrics.totalExpiries());
        check(metrics.totalMisses() == 2, "expired reads should count as misses, got " + metrics.totalMisses());
    }

    static void distributedMetricsCountEvictions() {
        ICache<Integer, Integer> cache = CacheBuilder.<Integer, Integer>newBuilder()
                .capacity(10).addNode("n1").addNode("n2").addNode("n3").metrics(true).buildDistributed();
        for (int i = 0; i < 300; i++) cache.put(i, i);

        ICacheMetrics metrics = (ICacheMetrics) cache;
        // 3 nodes x 10 capacity = 30 kept, so 270 must have been evicted
        check(cache.size() == 30, "expected 30 entries across 3 nodes, got " + cache.size());
        check(metrics.totalEvictions() == 270, "expected 270 evictions, got " + metrics.totalEvictions());
    }

    private static DistributedCache<String, String> distributedWith(String... nodes) {
        ConsistentHashMapper<String> mapper = new ConsistentHashMapper<>();
        for (String node : nodes) mapper.addNode(node);
        return new DistributedCache<>(mapper, addr -> nodeFactory().create(addr));
    }

    private static DistributedCache.NodeCacheFactory<String, String> nodeFactory() {
        return addr -> new service.LocalCache<>(new storage.ConcurrentStorage<>(),
                new eviction.LRUEvictionPolicy<>(100_000), 0);
    }

    static void addNodeMigratesKeys() {
        DistributedCache<String, String> cache = distributedWith("A", "B", "C");
        for (int i = 0; i < 3_000; i++) cache.put("k" + i, "v" + i);

        cache.addNode("D", nodeFactory());

        int missing = 0;
        for (int i = 0; i < 3_000; i++) if (!cache.get("k" + i).equals(java.util.Optional.of("v" + i))) missing++;
        check(missing == 0, missing + " keys were lost or wrong after adding a node");
        check(cache.size() == 3_000, "expected 3000 entries after rebalance, got " + cache.size());
    }

    static void addThenRemoveNodeHasNoStaleReads() {
        DistributedCache<String, String> cache = distributedWith("A");
        for (int i = 0; i < 1_000; i++) cache.put("k" + i, "old");

        cache.addNode("B", nodeFactory());
        for (int i = 0; i < 1_000; i++) cache.put("k" + i, "new");   // every key updated
        cache.removeNode("B");

        int stale = 0;
        for (int i = 0; i < 1_000; i++) if (!cache.get("k" + i).equals(java.util.Optional.of("new"))) stale++;
        check(stale == 0, stale + " keys returned a stale or missing value");
    }

    static void migrationPreservesTtl() throws Exception {
        DistributedCache<String, String> cache = distributedWith("A");
        for (int i = 0; i < 200; i++) cache.put("k" + i, "v", 150);

        cache.addNode("B", nodeFactory());   // ~half the keys move to B
        Thread.sleep(250);

        int alive = 0;
        for (int i = 0; i < 200; i++) if (cache.get("k" + i).isPresent()) alive++;
        check(alive == 0, alive + " keys outlived their TTL after migration");
    }

    static void sweepRemovesUnreadExpiredKeys() throws Exception {
        ICache<Integer, Integer> cache = CacheBuilder.<Integer, Integer>newBuilder()
                .capacity(1_000).ttl(50).ttlSweepInterval(25).buildLocal();
        for (int i = 0; i < 500; i++) cache.put(i, i);
        Thread.sleep(200);       // never read; only the sweep can remove them
        check(cache.size() == 0, "sweep should have removed all 500 expired keys, size=" + cache.size());
    }

    static void distributedRejectsSharedPolicy() {
        try {
            CacheBuilder.<String, String>newBuilder()
                    .evictionPolicy(new eviction.LRUEvictionPolicy<>(10)).addNode("A").addNode("B").buildDistributed();
        } catch (IllegalStateException expected) {
            return;
        }
        throw new AssertionError("expected IllegalStateException for a shared eviction policy");
    }

    // --- tiny test harness ---

    interface TestBody { void run() throws Exception; }

    private static void test(String name, TestBody body) {
        try {
            body.run();
            passed++;
            System.out.println("PASS  " + name);
        } catch (Throwable e) {
            failures.add(name + " -> " + e.getMessage());
            System.out.println("FAIL  " + name + " -> " + e.getMessage());
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
