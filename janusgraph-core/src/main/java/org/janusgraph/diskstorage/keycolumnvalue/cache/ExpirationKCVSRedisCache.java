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


import com.google.common.base.Preconditions;
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
import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ExpirationKCVSRedisCache extends KCVSCache {

    private static final int PENALTY_THRESHOLD = 5;
    public static final String REDIS_CACHE_PREFIX = "redis-cache-";
    public static final String REDIS_INDEX_CACHE_PREFIX = "redis-index-cache-";

    private final long cacheTimeMS;
    private final RMapCache<KeySliceQuery, byte[]> redisCache;
    private final RMapCache<StaticBuffer, ArrayList<KeySliceQuery>> redisIndexKeys;
    private static final Logger logger = LoggerFactory.getLogger(ExpirationKCVSRedisCache.class);
    private static final ObjectSerializer serializer = new JavaSerializer();

    public ExpirationKCVSRedisCache(final KeyColumnValueStore store, String metricsName, final long cacheTimeMS,
                                    final long invalidationGracePeriodMS, final long maximumByteSize, Configuration configuration) {
        super(store, metricsName);
        Preconditions.checkArgument(cacheTimeMS > 0, "Cache expiration must be positive: %s", cacheTimeMS);
        Preconditions.checkArgument(System.currentTimeMillis() + 1000L * 3600 * 24 * 365 * 100 + cacheTimeMS > 0, "Cache expiration time too large, overflow may occur: %s", cacheTimeMS);
        this.cacheTimeMS = cacheTimeMS;

        Preconditions.checkArgument(invalidationGracePeriodMS >= 0, "Invalid expiration grace period: %s", invalidationGracePeriodMS);

        RedissonClient redissonClient = RedissonCache.getRedissonClient(configuration);
        redisCache = redissonClient.getMapCache(REDIS_CACHE_PREFIX + metricsName);
        redisCache.setMaxSize(75000);

        redisIndexKeys = redissonClient.getMapCache(REDIS_INDEX_CACHE_PREFIX + metricsName);
        redisIndexKeys.setMaxSize(75000);

        logger.debug("********************** Configurations are loaded **********************");
    }

    @Override
    public EntryList getSlice(final KeySliceQuery query, final StoreTransaction txh) throws BackendException {
        try {
            incActionBy(1, CacheMetricsAction.RETRIEVAL, txh);

            byte[] bytQuery = redisCache.get(query);
            EntryList entries = serializer.deserialize(bytQuery);

            if (entries == null) {
                incActionBy(1, CacheMetricsAction.MISS, txh);
                return store.getSlice(query, unwrapTx(txh));
            } else {
                return entries;
            }

        } catch (Exception e) {
            if (e instanceof JanusGraphException) throw (JanusGraphException) e;
            else if (e.getCause() instanceof JanusGraphException) throw (JanusGraphException) e.getCause();
            else throw new JanusGraphException(e);
        }
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

            bytResult = redisCache.get(ksqs[i]);

            if (bytResult != null) {
                result = bytResult != null ? (EntryList) serializer.deserialize(bytResult) : null;
            } else {
                ksqs[i] = null;
            }

            if (result != null) {
                results.put(key, result);
            } else {
                remainingKeys.add(key);
            }
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
                        putToRedis(ksqs[i], subresult);
                    }
                }
            }
        }
        return results;
    }

    private void putToRedis(KeySliceQuery keySliceQuery, EntryList entries) {
        try {
            CompletableFuture<Boolean> putAsyncFuture = redisCache.fastPutAsync(keySliceQuery, serializer.serialize(entries), cacheTimeMS, TimeUnit.MILLISECONDS)
                .toCompletableFuture();
            putIndexKeysToRedis(keySliceQuery);
            putAsyncFuture.get();
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for put data async to Redis", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Put async to Redis failed ", e);
        }
    }

    private void putIndexKeysToRedis(KeySliceQuery keySliceQuery) {
        RLock lock = redisIndexKeys.getLock(keySliceQuery.getKey());
        try {
            if (lock.tryLock(1, 2, TimeUnit.SECONDS)) {
                ArrayList<KeySliceQuery> queryList = redisIndexKeys.get(keySliceQuery.getKey());
                if (queryList == null) {
                    queryList = new ArrayList<>();
                }
                queryList.add(keySliceQuery);
                redisIndexKeys.fastPut(keySliceQuery.getKey(), queryList, cacheTimeMS, TimeUnit.MILLISECONDS);
            } else {
                logger.warn("Failed to acquire lock for key {}", keySliceQuery.getKey());
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted while acquiring lock from Redis", e);
            Thread.currentThread().interrupt();
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            } else {
                logger.warn("Lock not held by current thread, skipping unlock");
            }
        }
    }

    @Override
    public void clearCache() {
        redisCache.clearExpire();
    }

    public void invalidate(StaticBuffer key, List<CachableStaticBuffer> entries) {
        logger.debug("Invalidating key {}", key);
        RLock lock = redisIndexKeys.getLock(key);
        try {
            if (lock.tryLock(1, 2, TimeUnit.SECONDS)) {
                List<KeySliceQuery> keySliceQueryList = redisIndexKeys.get(key);
                if (keySliceQueryList != null) {
                    List<KeySliceQuery> keySliceQueryListCopy = new ArrayList<>(keySliceQueryList);
                    for (KeySliceQuery keySliceQuery : keySliceQueryListCopy) {
                        if (key.equals(keySliceQuery.getKey())) {
                            redisCache.remove(keySliceQuery);
                        }
                    }
                }
            } else {
                logger.warn("Failed to acquire lock for key {}", key);
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted while acquiring lock from Redis", e);
            Thread.currentThread().interrupt();
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            } else {
                logger.warn("Lock not held by current thread, skipping unlock");
            }
        }
    }

    @Override
    public void close() throws BackendException {
        super.close();
    }

    private interface ObjectSerializer {
        <T> byte[] serialize(T obj);

        <T> T deserialize(byte[] bytes);
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
}
