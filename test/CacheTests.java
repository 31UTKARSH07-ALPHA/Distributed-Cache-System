import api.ICache;
import factory.CacheBuilder;

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
