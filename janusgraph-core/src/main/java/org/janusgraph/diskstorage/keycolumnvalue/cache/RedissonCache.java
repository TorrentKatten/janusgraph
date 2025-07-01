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

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_MIN_IDLE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_POOL_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_TIME_OUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_HOST;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_KEEP_ALIVE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_PORT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_REPLICA_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_REPLICA_PORTS;

public class RedissonCache {
    private RedissonCache() {
    }

    private static class RedissonClientFactoryHolder {
        private static RedissonClient redisson;

        private static RedissonClient initRedisson(Configuration configuration) {
            int redisCachePort = configuration.get(REDIS_CACHE_PORT);
            int connectionPoolSize = configuration.get(REDIS_CACHE_CONNECTION_POOL_SIZE);
            int connectionMinimumIdleSize = configuration.get(REDIS_CACHE_CONNECTION_MIN_IDLE_SIZE);
            int connectTimeout = configuration.get(REDIS_CACHE_CONNECTION_TIME_OUT);
            boolean keepAlive = configuration.get(REDIS_CACHE_KEEP_ALIVE);

            String address = "redis://" + configuration.get(REDIS_CACHE_HOST) + ":" + redisCachePort;
            Config config;
            Optional<String> replicaHostsValue = Optional.ofNullable(configuration.get(REDIS_CACHE_REPLICA_HOSTS));
            if (replicaHostsValue.isPresent() && StringUtils.isNotBlank(replicaHostsValue.get())) {
                config = createReplicatedServersConfig(replicaHostsValue.get(), configuration.get(REDIS_CACHE_REPLICA_PORTS),
                    address, redisCachePort, connectionPoolSize, connectionMinimumIdleSize, connectTimeout, keepAlive);
            } else {
                config = createSingleServerConfig(address, connectionPoolSize, connectionMinimumIdleSize, connectTimeout, keepAlive);
            }

            RedissonClient client = Redisson.create(config);
            Runtime.getRuntime().addShutdownHook(new Thread(client::shutdown));
            return client;
        }

        private static Config createSingleServerConfig(String address, int connectionPoolSize, int connectionMinimumIdleSize,
                                                       int connectTimeout, boolean keepAlive) {
            Config config = new Config();
            config.useSingleServer()
                .setAddress(address)
                .setConnectionPoolSize(connectionPoolSize)
                .setConnectionMinimumIdleSize(connectionMinimumIdleSize)
                .setConnectTimeout(connectTimeout)
                .setKeepAlive(keepAlive);

            return config;
        }

        private static Config createReplicatedServersConfig(String replicaHostsValue, String replicaPortsValue, String masterAddress,
                                                            int masterCachePort, int connectionPoolSize, int connectionMinimumIdleSize,
                                                            int connectTimeout, boolean keepAlive) {
            List<String> nodeAddresses = new ArrayList<>();
            List<String> replicaPorts = Optional.ofNullable(replicaPortsValue)
                .map(ports -> Arrays.stream(ports.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList()))
                .orElse(ImmutableList.of());
            List<String> replicaHosts = Arrays.stream(replicaHostsValue.split(","))
                .map(String::trim)
                .map(host -> "redis://" + host)
                .collect(Collectors.toList());
            if (!replicaPorts.isEmpty() && replicaPorts.size() != replicaHosts.size()) {
                throw new IllegalArgumentException("Number of replica hosts and ports must match");
            }

            nodeAddresses.add(masterAddress);
            for (int i = 0; i < replicaHosts.size(); i++) {
                String replicaUri = replicaHosts.get(i);
                replicaUri += ":" + (replicaPorts.isEmpty() ? masterCachePort : replicaPorts.get(i));
                nodeAddresses.add(replicaUri);
            }

            Config config = new Config();
            config.useReplicatedServers()
                .addNodeAddress(nodeAddresses.toArray(new String[0]))
                .setReadMode(ReadMode.MASTER_SLAVE)
                .setConnectTimeout(connectTimeout)
                .setKeepAlive(keepAlive)
                .setMasterConnectionPoolSize(connectionPoolSize)
                .setMasterConnectionMinimumIdleSize(connectionMinimumIdleSize)
                .setSlaveConnectionPoolSize(connectionPoolSize)
                .setSlaveConnectionMinimumIdleSize(connectionMinimumIdleSize);
            return config;
        }
    }

    public static synchronized RedissonClient getRedissonClient(Configuration configuration) {
        if (RedissonClientFactoryHolder.redisson == null) {
            RedissonClientFactoryHolder.redisson = RedissonClientFactoryHolder.initRedisson(configuration);
        }
        return RedissonClientFactoryHolder.redisson;
    }
}
