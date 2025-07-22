package io.greptime.bench;

import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.models.DataType;
import io.greptime.models.TableSchema;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class BulkMetricsTableDataProvider implements TableDataProvider {

    private final TableSchema tableSchema;
    private final long rowCount;
    private final int serviceNumPerApp;

    /*
    ```sql
    CREATE TABLE `tt_metrics_table` (
        `ts` TIMESTAMP(3) NOT NULL,
        `idc` STRING NULL INVERTED INDEX,
        `host` STRING NULL INVERTED INDEX,
        `shard` INT,
        `service` STRING NULL INVERTED INDEX,
        `url` STRING NULL,
        `cpu_util` DOUBLE NULL,
        `memory_util` DOUBLE NULL,
        `disk_util` DOUBLE NULL,
        `load_util` DOUBLE NULL,
        `session_id` STRING NULL,
        TIME INDEX (`ts`),
    )
    PARTITION ON COLUMNS (shard) (
        shard < 1,
        shard >= 1
    )
    ENGINE=mito
    WITH(
        append_mode = 'true',
        skip_wal = 'true',
    );
    ```
    */

    {
        this.tableSchema = TableSchema.newBuilder("tt_metrics_table")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addTag("idc", DataType.String)
                .addTag("host", DataType.String)
                .addField("shard", DataType.Int32)
                .addTag("service", DataType.String)
                .addField("url", DataType.String)
                .addField("cpu_util", DataType.Float64)
                .addField("memory_util", DataType.Float64)
                .addField("disk_util", DataType.Float64)
                .addField("load_util", DataType.Float64)
                .addField("session_id", DataType.String)
                .build();
        this.rowCount = SystemPropertyUtil.getLong("table_row_count", 5_000_000_000L);
        this.serviceNumPerApp = SystemPropertyUtil.getInt("tt_metrics_table.service_num_per_app", 20);
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
                String idc = nextIdc(random);
                String host = nextHost(random, idc);
                String app = nextApp(random, host);
                String url = nextUrl(random, ts);
                String service = nextService(random, app, (int) (index % serviceNumPerApp));
                String sessionId = nextSessionId(random);

                return new Object[] {
                    ts,
                    idc,
                    host,
                    0,
                    service,
                    url,
                    random.nextDouble(0.1, 0.2), // cpu_util
                    random.nextDouble(0.3, 0.4), // memory_util
                    random.nextDouble(0.6, 0.7), // disk_util
                    random.nextDouble(8.8, 8.9), // load_util
                    sessionId
                };
            }
        };
    }

    @Override
    public long rowCount() {
        return this.rowCount;
    }

    /**
     * Returns a random idc, there are 20 idcs.
     */
    private String nextIdc(ThreadLocalRandom random) {
        return "idc_" + random.nextInt(20);
    }

    /**
     * Returns a random host name. Each IDC contains approximately 500 hosts, with a total of 10,000 hosts across all IDCs.
     */
    private String nextHost(ThreadLocalRandom random, String idc) {
        return idc + "_host_" + random.nextInt(500);
    }

    /**
     * Returns a random app name. There are 500 apps across all IDCs.
     */
    private String nextApp(ThreadLocalRandom random, String host) {
        int hash = host.hashCode();
        return "app_" + (hash % 500);
    }

    /**
     * Returns a random service name. Each app contains 20 services.
     */
    private String nextService(ThreadLocalRandom random, String app, int index) {
        return app + "_service_" + index;
    }

    /**
     * Returns a random URL with a timestamp-based path.
     * The URL format is: http://127.0.0.1/helloworld/{minutes}/{random_id}
     * where random_id is between 0 and 1999.
     */
    private String nextUrl(ThreadLocalRandom random, long ts) {
        long minutes = TimeUnit.MILLISECONDS.toMinutes(ts);
        return String.format("http://127.0.0.1/helloworld/%d/%d", minutes, random.nextInt(2000));
    }

    private String nextSessionId(ThreadLocalRandom random) {
        return "session_" + random.nextInt(1000_0000);
    }
}
