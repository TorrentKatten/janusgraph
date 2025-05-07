// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.keycolumnvalue.cache;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.CacheMetricsAction;
import org.nustaq.serialization.FSTConfiguration;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.util.datastructures.ByteSize.OBJECT_HEADER;
import static org.janusgraph.util.datastructures.ByteSize.OBJECT_REFERENCE;
import static org.janusgraph.util.datastructures.ByteSize.STATICARRAYBUFFER_RAW_SIZE;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpirationKCVSRedisCache extends KCVSCache {

    //Weight estimation
    private static final int STATIC_ARRAY_BUFFER_SIZE = STATICARRAYBUFFER_RAW_SIZE + 10; // 10 = last number is average length
    private static final int KEY_QUERY_SIZE = OBJECT_HEADER + 4 + 1 + 3 * (OBJECT_REFERENCE + STATIC_ARRAY_BUFFER_SIZE); // object_size + int + boolean + 3 static buffers

    private static final int INVALIDATE_KEY_FRACTION_PENALTY = 1000;
    private static final int PENALTY_THRESHOLD = 5;
    public static final String REDIS_CACHE_PREFIX = "redis-cache-";
    public static final String REDIS_INDEX_CACHE_PREFIX = "redis-index-cache-";

    private volatile CountDownLatch penaltyCountdown;

    private final ConcurrentHashMap<StaticBuffer, Long> expiredKeys;

    private final long cacheTimeMS;
    private final long invalidationGracePeriodMS;
    private final CleanupThread cleanupThread;
    private final RLocalCachedMap<KeySliceQuery, byte[]> redisCache;
    private final RLocalCachedMap<StaticBuffer, ArrayList<KeySliceQuery>> redisIndexKeys;
    private static final Logger logger = LoggerFactory.getLogger("redis-logger");
    private static final ObjectSerializer serializer = new JavaSerializer();
//    private static final ObjectSerializer serializer = new FstSerializer();
//    private static final ObjectSerializer serializer = new JsonSerializer();

    public ExpirationKCVSRedisCache(final KeyColumnValueStore store, String metricsName, final long cacheTimeMS,
                                    final long invalidationGracePeriodMS, final long maximumByteSize, Configuration configuration) {
        super(store, metricsName);
        Preconditions.checkArgument(cacheTimeMS > 0, "Cache expiration must be positive: %s", cacheTimeMS);
        Preconditions.checkArgument(System.currentTimeMillis() + 1000L * 3600 * 24 * 365 * 100 + cacheTimeMS > 0, "Cache expiration time too large, overflow may occur: %s", cacheTimeMS);
        this.cacheTimeMS = cacheTimeMS;
        final int concurrencyLevel = Runtime.getRuntime().availableProcessors();
        Preconditions.checkArgument(invalidationGracePeriodMS >= 0, "Invalid expiration grace period: %s", invalidationGracePeriodMS);
        this.invalidationGracePeriodMS = invalidationGracePeriodMS;

        RedissonClient redissonClient = RedissonCache.getRedissonClient(configuration);
        redisCache = redissonClient.getLocalCachedMap(REDIS_CACHE_PREFIX + metricsName, LocalCachedMapOptions.defaults());
        redisIndexKeys = redissonClient.getLocalCachedMap(REDIS_INDEX_CACHE_PREFIX + metricsName, LocalCachedMapOptions.defaults());
        expiredKeys = new ConcurrentHashMap<>(50, 0.75f, concurrencyLevel);
        penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);

        cleanupThread = new CleanupThread();
        cleanupThread.start();
        logger.debug("********************** Configurations are loaded **********************");
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        incActionBy(1, CacheMetricsAction.RETRIEVAL, txh);
        if (isExpired(query)) {
            incActionBy(1, CacheMetricsAction.MISS, txh);
            return store.getSlice(query, unwrapTx(txh));
        }

        try {
            return get(query, () -> {
                incActionBy(1, CacheMetricsAction.MISS, txh);
                return store.getSlice(query, unwrapTx(txh));
            });
        } catch (Exception e) {
            if (e instanceof JanusGraphException) throw (JanusGraphException) e;
            else if (e.getCause() instanceof JanusGraphException) throw (JanusGraphException) e.getCause();
            else throw new JanusGraphException(e);
        }
    }

    private EntryList get(KeySliceQuery query, Callable<EntryList> valueLoader) {
        byte[] bytQuery = redisCache.get(query);
        EntryList entries = serializer.deserialize(bytQuery);
        if (entries == null) {
            logger.debug("reading from the store.................");
            try {
                entries = valueLoader.call();
                if (entries == null) {
                    throw new CacheLoader.InvalidCacheLoadException("valueLoader must not return null, key=" + query);
                } else {
                    redisCache.fastPutAsync(query, serializer.serialize(entries));
                    RLock lock = redisIndexKeys.getLock(query.getKey());
                    try {
                        lock.tryLock(1, 2, TimeUnit.SECONDS);
                        ArrayList<KeySliceQuery> queryList = redisIndexKeys.get(query.getKey());
                        if (queryList == null)
                            queryList = new ArrayList<>();
                        queryList.add(query);
                        redisIndexKeys.fastPutAsync(query.getKey(), queryList);
                    } catch (InterruptedException e) {
                        logger.warn("Interrupted while waiting for lock", e);
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to read from the store.", e);
            }
        }
        return entries;
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(final List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws BackendException {
        final Map<StaticBuffer, EntryList> results = new HashMap<>(keys.size());
        final List<StaticBuffer> remainingKeys = new ArrayList<>(keys.size());
        KeySliceQuery[] ksqs = new KeySliceQuery[keys.size()];
        incActionBy(keys.size(), CacheMetricsAction.RETRIEVAL, txh);
        byte[] bytResult;
        //Find all cached queries
        for (int i = 0; i < keys.size(); i++) {
            final StaticBuffer key = keys.get(i);
            ksqs[i] = new KeySliceQuery(key, query);
            EntryList result = null;
            if (!isExpired(ksqs[i])) {
                bytResult = redisCache.get(ksqs[i]);
                result = bytResult != null ? (EntryList) serializer.deserialize(bytResult) : null;
            } else ksqs[i] = null;
            if (result != null) results.put(key, result);
            else remainingKeys.add(key);
        }
        //Request remaining ones from backend
        if (!remainingKeys.isEmpty()) {
            incActionBy(remainingKeys.size(), CacheMetricsAction.MISS, txh);
            Map<StaticBuffer, EntryList> subresults = store.getSlice(remainingKeys, query, unwrapTx(txh));

            for (int i = 0; i < keys.size(); i++) {
                StaticBuffer key = keys.get(i);
                EntryList subresult = subresults.get(key);
                if (subresult != null) {
                    results.put(key, subresult);
                    if (ksqs[i] != null) {
                        logger.info("adding to cache subresult {}", subresult);
                        redisCache.fastPutAsync(ksqs[i], serializer.serialize(subresult));
                        RLock lock = redisIndexKeys.getLock(ksqs[i].getKey());
                        try {
                            lock.tryLock(1, 2, TimeUnit.SECONDS);
                            ArrayList<KeySliceQuery> queryList = redisIndexKeys.get(ksqs[i].getKey());
                            if (queryList == null)
                                queryList = new ArrayList<>();
                            queryList.add(ksqs[i]);
                            redisIndexKeys.fastPut(ksqs[i].getKey(), queryList);
                        } catch (InterruptedException e) {
                            logger.warn("Interrupted while waiting for lock", e);
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            }
        }
        return results;
    }

    @Override
    public void clearCache() {
        redisCache.clearExpire();
        expiredKeys.clear();
        penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);
    }

    @Override
    public void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries) {
        logger.info("Invalidating key {}, get lock in thread {}", key, Thread.currentThread().getId());
        RLock lock = redisIndexKeys.getLock(key);
        if (lock.isHeldByCurrentThread()) {
            logger.info("Lock already held by current thread {}, proceeding with invalidation", Thread.currentThread().getId());
            this.invalidateInternal(key, entries);
        } else {
            try {
                if (lock.tryLock(1L, 2L, TimeUnit.SECONDS)) {
                    logger.info("Lock acquired, held by thread {}, proceeding with invalidation", Thread.currentThread().getId());
                    this.invalidateInternal(key, entries);
                } else {
                    logger.warn("Failed to acquire lock for key {}, held by another thread", key);
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for lock", e);
                Thread.currentThread().interrupt();
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    logger.info("Releasing lock held by thread {}", Thread.currentThread().getId());
                    lock.unlock();
                }
            }
        }
    }

    private void invalidateInternal(StaticBuffer key, List<CachableStaticBuffer> entries) {
        List<KeySliceQuery> keySliceQueryList = redisIndexKeys.get(key);
        if (keySliceQueryList != null) {
            for (KeySliceQuery keySliceQuery : keySliceQueryList) {
                if (key.equals(keySliceQuery.getKey())) {
                    redisCache.fastRemove(keySliceQuery);
                }
            }

            Preconditions.checkArgument(!hasValidateKeysOnly() || entries.isEmpty());
            expiredKeys.put(key, getExpirationTime());
            if (Math.random() < 1.0 / INVALIDATE_KEY_FRACTION_PENALTY) penaltyCountdown.countDown();
        }
    }

    @Override
    public void close() throws BackendException {
        cleanupThread.stopThread();
        super.close();
    }

    private boolean isExpired(final KeySliceQuery query) {
        Long until = expiredKeys.get(query.getKey());
        if (until == null) return false;
        if (isBeyondExpirationTime(until)) {
            expiredKeys.remove(query.getKey(), until);
            return false;
        }
        //We suffer a cache miss, hence decrease the count down
        penaltyCountdown.countDown();
        return true;
    }

    private long getExpirationTime() {
        return System.currentTimeMillis() + cacheTimeMS;
    }

    private boolean isBeyondExpirationTime(long until) {
        return until < System.currentTimeMillis();
    }

    private class CleanupThread extends Thread {

        private boolean stop = false;

        public CleanupThread() {
            this.setDaemon(true);
            this.setName("ExpirationStoreCache-" + getId());
        }

        @Override
        public void run() {
            while (true) {
                if (stop) return;
                try {

                    penaltyCountdown.await();
                } catch (InterruptedException e) {
                    if (stop) return;
                    else throw new RuntimeException("Cleanup thread got interrupted", e);
                }
                //Do clean up work by invalidating all entries for expired keys
                final Map<StaticBuffer, Long> expiredKeysCopy = new HashMap<>(expiredKeys.size());
                for (Map.Entry<StaticBuffer, Long> expKey : expiredKeys.entrySet()) {
                    if (isBeyondExpirationTime(expKey.getValue()))
                        expiredKeys.remove(expKey.getKey(), expKey.getValue());
                    else if (getAge(expKey.getValue()) >= invalidationGracePeriodMS)
                        expiredKeysCopy.put(expKey.getKey(), expKey.getValue());
                }
                for (KeySliceQuery ksq : redisCache.keySet()) {
                    if (expiredKeysCopy.containsKey(ksq.getKey())) redisCache.remove(ksq);
                }
                penaltyCountdown = new CountDownLatch(PENALTY_THRESHOLD);
                for (Map.Entry<StaticBuffer, Long> expKey : expiredKeysCopy.entrySet()) {
                    expiredKeys.remove(expKey.getKey(), expKey.getValue());
                }
            }
        }

        void stopThread() {
            stop = true;
            this.interrupt();
        }

        private long getAge(long until) {
            long age = System.currentTimeMillis() - (until - cacheTimeMS);
            assert age >= 0;
            return age;
        }
    }

    private interface ObjectSerializer {
        <T> byte[] serialize(T obj);

        <T> T deserialize(byte[] bytes);
    }

    private static class JsonSerializer implements ObjectSerializer {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public <T> byte[] serialize(T obj) {
            if (obj == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(obj);
            } catch (IOException e) {
                logger.warn("Failed to serialize object", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public <T> T deserialize(byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            try {
                return objectMapper.readValue(bytes, new TypeReference<T>() {
                });
            } catch (IOException e) {
                logger.warn("Failed to deserialize object", e);
                throw new RuntimeException(e);
            }
        }

    }

    private static class JavaSerializer implements ObjectSerializer {
        @Override
        public <T> byte[] serialize(T obj) {
            if (obj == null) {
                return null;
            }
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(obj);
                return baos.toByteArray();
            } catch (IOException e) {
                logger.warn("Failed to serialize object", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public <T> T deserialize(byte[] bytes) {
            if (bytes == null) {
                return null;
            }
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 ObjectInputStream ois = new ObjectInputStream(bais)) {
                return (T) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                logger.warn("Failed to deserialize object", e);
                throw new RuntimeException(e);
            }
        }
    }

    private static class FstSerializer implements ObjectSerializer {
        private static final FSTConfiguration fastConf = FSTConfiguration.createDefaultConfiguration();

        @Override
        public <T> byte[] serialize(T obj) {
            return fastConf.asByteArray(obj);
        }

        @Override
        public <T> T deserialize(byte[] bytes) {
            return (T) (bytes != null ? fastConf.asObject(bytes) : null);
        }

    }

}
