package ttl;
import storage.CacheEntry;
import storage.IStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

// Finds expired entries; the owning cache removes them (under its lock) via the ExpiryHandler,
// so the sweep thread never touches the cache's eviction state directly.
public class TTLManager<K, V> implements AutoCloseable {
    private static final Logger log = Logger.getLogger(TTLManager.class.getName());

    public interface ExpiryHandler<K> {
        void expireIfStale(K key);
    }

    private final IStorage<K, V> storage;
    private final ExpiryHandler<K> expiryHandler;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> sweepTask;

    public TTLManager(IStorage<K, V> storage, ExpiryHandler<K> expiryHandler) {
        this.storage       = storage;
        this.expiryHandler = expiryHandler;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-ttl-sweep");
            t.setDaemon(true);
            return t;
        });
    }

    public void startScheduledSweep(long intervalMillis) {
        if (sweepTask != null) sweepTask.cancel(false);
        sweepTask = scheduler.scheduleAtFixedRate(
                this::sweep, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS
        );
        log.fine("TTL sweep scheduled every " + intervalMillis + "ms");
    }
    public void sweep() {
        try {
            List<K> expired = new ArrayList<>();
            for (CacheEntry<K, V> entry : storage.entries()) {
                if (entry.isExpired()) expired.add(entry.getKey());
            }
            for (K key : expired) {
                expiryHandler.expireIfStale(key);
            }
            if (!expired.isEmpty()) {
                log.fine(() -> "TTL sweep removed " + expired.size() + " expired entries");
            }
        } catch (Exception e) {
            // An exception would silently cancel scheduleAtFixedRate, so log and keep sweeping.
            log.log(Level.WARNING, "TTL sweep failed", e);
        }
    }

    @Override
    public void close() {
        if (sweepTask != null) sweepTask.cancel(false);
        scheduler.shutdownNow();
    }
}
