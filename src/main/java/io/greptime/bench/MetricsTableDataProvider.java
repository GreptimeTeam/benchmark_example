package io.greptime.bench;

import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.models.DataType;
import io.greptime.models.TableSchema;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The metrics table data provider.
 *
 * Create a table with the following schema:
 *
 * ```sql
 * CREATE TABLE tt_metrics_table (
 *     ts TIMESTAMP TIME INDEX,
 *     idc STRING INVERTED INDEX,
 *     host STRING INVERTED INDEX,
 *     service STRING INVERTED INDEX,
 *     cpu_util FLOAT64,
 *     memory_util FLOAT64,
 *     disk_util FLOAT64,
 *     load_util FLOAT64,
 *     PRIMARY KEY (idc, host, service)
 * )
 * ENGINE=mito
 * WITH('append_mode'='true');
 * ```
 */
public class MetricsTableDataProvider implements TableDataProvider {

    private final TableSchema tableSchema;
    private final long rowCount;
    private final int serviceNumPerApp;

    {
        this.tableSchema = TableSchema.newBuilder("tt_metrics_table")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addTag("idc", DataType.String)
                .addTag("host", DataType.String)
                .addTag("service", DataType.String)
                .addField("cpu_util", DataType.Float64)
                .addField("memory_util", DataType.Float64)
                .addField("disk_util", DataType.Float64)
                .addField("load_util", DataType.Float64)
                .build();
        this.rowCount = SystemPropertyUtil.getLong("tt_metrics_table.row_count", 10_000_000_000L);
        this.serviceNumPerApp = SystemPropertyUtil.getInt("tt_metrics_table.service_num_per_app", 20);
    }

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public void close() throws Exception {
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
            private Batch batch = null;

            @Override
            public boolean hasNext() {
                return index < rowCount;
            }

            @Override
            public Object[] next() {
                index++;

                if (batch == null || !batch.hasNext()) {
                    // fill data
                    ArrayList<Object[]> rows = new ArrayList<>(serviceNumPerApp);
                    long ts = System.currentTimeMillis();

                    String idc = nextIdc(random);
                    String host = nextHost(random, idc);
                    String app = nextApp(random, host);

                    // In real-world scenarios, all services on a host typically generate metrics data simultaneously,
                    // so this data generation logic aligns with real-world patterns.
                    //
                    // Each app/host has 20 services.
                    for (int i = 0; i < serviceNumPerApp; i++) {
                        String service = nextService(random, app, i);

                        rows.add(new Object[] {
                            ts,
                            idc,
                            host,
                            service,
                            random.nextDouble(0, 100), // cpu_util
                            random.nextDouble(0, 100), // memory_util
                            random.nextDouble(0, 100), // disk_util
                            random.nextDouble(0, 100), // load_util
                        });
                    }

                    batch = new Batch(rows);
                }

                return batch.next();
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
}

class Batch {
    private final ArrayList<Object[]> rows;
    private int index = 0;

    Batch(ArrayList<Object[]> rows) {
        this.rows = rows;
    }

    boolean hasNext() {
        return this.index < this.rows.size();
    }

    Object[] next() {
        return this.rows.get(this.index++);
    }
}
