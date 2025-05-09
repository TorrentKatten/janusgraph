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

import org.janusgraph.diskstorage.configuration.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_MIN_IDLE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_POOL_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_TIME_OUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_HOST;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_KEEP_ALIVE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_PORT;

public class RedissonCache {

    private static class RedissonClientFactoryHolder {
        private static RedissonClient redisson;

        private static RedissonClient initRedisson(Configuration configuration) {
            String redisCacheHost = configuration.get(REDIS_CACHE_HOST);
            int redisCachePort = configuration.get(REDIS_CACHE_PORT);
            int connectionPoolSize = configuration.get(REDIS_CACHE_CONNECTION_POOL_SIZE);
            int connectionMinimumIdleSize = configuration.get(REDIS_CACHE_CONNECTION_MIN_IDLE_SIZE);
            int connectTimeout = configuration.get(REDIS_CACHE_CONNECTION_TIME_OUT);
            boolean keepAlive = configuration.get(REDIS_CACHE_KEEP_ALIVE);

            Config config = new Config();
            config.useSingleServer().setAddress("redis://" + redisCacheHost + ":" + redisCachePort).setConnectionPoolSize(connectionPoolSize)
                .setConnectionMinimumIdleSize(connectionMinimumIdleSize)
                .setConnectTimeout(connectTimeout)
                .setKeepAlive(keepAlive);
            RedissonClient client = Redisson.create(config);
            Runtime.getRuntime().addShutdownHook(new Thread(client::shutdown));
            return client;
        }
    }

    private RedissonCache() {
    }

    public static synchronized RedissonClient getRedissonClient(Configuration configuration) {
        if (RedissonClientFactoryHolder.redisson == null) {
            RedissonClientFactoryHolder.redisson = RedissonClientFactoryHolder.initRedisson(configuration);
        }
        return RedissonClientFactoryHolder.redisson;
    }
}
