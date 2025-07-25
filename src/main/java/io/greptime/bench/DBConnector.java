/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.bench;

import io.greptime.GreptimeDB;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.options.GreptimeOptions;
import io.greptime.rpc.RpcOptions;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DBConnector is a helper class to connect to a GreptimeDB instance.
 */
public class DBConnector {

    private static final Logger LOG = LoggerFactory.getLogger(DBConnector.class);

    public static GreptimeDB connect() {
        Properties prop = new Properties();
        try {
            prop.load(DBConnector.class.getResourceAsStream("/db-connection.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String database = SystemPropertyUtil.get("db.database");
        if (database == null) {
            database = (String) prop.get("db.database");
        }

        String endpointsStr = SystemPropertyUtil.get("db.endpoints");
        if (endpointsStr == null) {
            endpointsStr = prop.getProperty("db.endpoints");
        }

        String[] endpoints = endpointsStr.split(",");
        RpcOptions rpcOptions = RpcOptions.newDefault();
        rpcOptions.setDefaultRpcTimeout(60 * 1000);
        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database)
                .writeMaxRetries(0)
                .rpcOptions(rpcOptions)
                .defaultStreamMaxWritePointsPerSecond(Integer.MAX_VALUE)
                .maxInFlightWritePoints(Integer.MAX_VALUE)
                .useZeroCopyWriteInBulkWrite(true)
                .build();
        LOG.info("Connect to db: {}, endpoint: {}", database, endpointsStr);

        return GreptimeDB.create(opts);
    }
}
