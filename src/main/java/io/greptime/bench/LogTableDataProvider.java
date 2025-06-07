package io.greptime.bench;

import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.models.DataType;
import io.greptime.models.TableSchema;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The log table data provider.
 */
public class LogTableDataProvider implements TableDataProvider {

    private final TableSchema tableSchema;
    private final long rowCount;

    /*
     CREATE TABLE IF NOT EXISTS `tt_log_table` (
    `ts` TIMESTAMP(3) NOT NULL,
    `log_uid` STRING NULL SKIPPING INDEX,
    `log_message` STRING NULL,
    `log_status` STRING NULL SKIPPING INDEX,
    `p` STRING NULL,
    `host_id` BIGINT NULL SKIPPING INDEX,
    `host_name` STRING NULL,
    `service_id` BIGINT NULL SKIPPING INDEX,
    `service_name` STRING NULL,
    `service_instance_id` BIGINT NULL SKIPPING INDEX,
    `service_instance_name` STRING NULL,
    `container_id` BIGINT NULL SKIPPING INDEX,
    `container_name` STRING NULL,
    `pod_id` BIGINT NULL SKIPPING INDEX,
    `pod_name` STRING NULL,
    `cluster_id` BIGINT NULL SKIPPING INDEX,
    `cluster_name` STRING NULL,
    `node_id` BIGINT NULL SKIPPING INDEX,
    `node_name` STRING NULL,
    `ns_id` BIGINT NULL SKIPPING INDEX,
    `ns_name` STRING NULL,
    `workload_id` BIGINT NULL SKIPPING INDEX,
    `workload_name` STRING NULL,
    TIME INDEX (`ts`)
) ENGINE=mito
WITH(
  append_mode = 'true',
  skip_wal = 'true',
);
     */

    {
        this.tableSchema = TableSchema.newBuilder("tt_log_table")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("log_uid", DataType.String)
                .addField("log_message", DataType.String)
                .addField("log_status", DataType.String)
                .addField("p", DataType.String)
                .addField("host_id", DataType.Int64)
                .addField("host_name", DataType.String)
                .addField("service_id", DataType.Int64)
                .addField("service_name", DataType.String)
                .addField("service_instance_id", DataType.Int64)
                .addField("service_instance_name", DataType.String)
                .addField("container_id", DataType.Int64)
                .addField("container_name", DataType.String)
                .addField("pod_id", DataType.Int64)
                .addField("pod_name", DataType.String)
                .addField("cluster_id", DataType.Int64)
                .addField("cluster_name", DataType.String)
                .addField("node_id", DataType.Int64)
                .addField("node_name", DataType.String)
                .addField("ns_id", DataType.Int64)
                .addField("ns_name", DataType.String)
                .addField("workload_id", DataType.Int64)
                .addField("workload_name", DataType.String)
                .build();
        this.rowCount = SystemPropertyUtil.getLong("tt_log_table.row_count", 5_000_000_000L);
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public TableSchema tableSchema() {
        return this.tableSchema;
    }

    @Override
    public Iterator<Object[]> rows() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        return new Iterator<Object[]>() {

            private long index = 0;

            @Override
            public boolean hasNext() {
                return index < rowCount;
            }

            @Override
            public Object[] next() {
                index++;

                long ts = System.currentTimeMillis();
                String logUid = UUID.randomUUID().toString();
                String logMessage = LogTextHelper.generate1500Text(random, ts);
                String logStatus = "log_status_" + random.nextInt(10);
                String p = "p_" + random.nextInt(10);
                Object[] host = nextIdWithName(random, "host", 10000);
                Object[] service = nextIdWithName(random, "service", 100000);
                Object[] serviceInstance = nextIdWithName(random, "service_instance", 100000);
                Object[] container = nextIdWithName(random, "container", 100000);
                Object[] pod = nextIdWithName(random, "pod", 100000);
                Object[] cluster = nextIdWithName(random, "cluster", 100);
                Object[] node = nextIdWithName(random, "node", 100000);
                Object[] ns = nextIdWithName(random, "ns", 100);
                Object[] workload = nextIdWithName(random, "workload", 10);

                return new Object[] {
                    ts,
                    logUid,
                    logMessage,
                    logStatus,
                    p,
                    host[0],
                    host[1],
                    service[0],
                    service[1],
                    serviceInstance[0],
                    serviceInstance[1],
                    container[0],
                    container[1],
                    pod[0],
                    pod[1],
                    cluster[0],
                    cluster[1],
                    node[0],
                    node[1],
                    ns[0],
                    ns[1],
                    workload[0],
                    workload[1]
                };
            }
        };
    }

    private Object[] nextIdWithName(ThreadLocalRandom random, String prefix, int base) {
        long id = random.nextLong(base);
        String name = prefix + "_" + id;
        return new Object[] {id, name};
    }

    @Override
    public long rowCount() {
        return this.rowCount;
    }
}
