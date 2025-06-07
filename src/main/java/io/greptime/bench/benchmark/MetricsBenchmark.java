package io.greptime.bench.benchmark;

import io.greptime.GreptimeDB;
import io.greptime.WriteOp;
import io.greptime.bench.DBConnector;
import io.greptime.bench.MetricsTableDataProvider;
import io.greptime.bench.TableDataProvider;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.metrics.ExporterOptions;
import io.greptime.metrics.MetricsExporter;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsBenchmark.class);

    public static void main(String[] args) throws Exception {
        boolean zstdCompression = SystemPropertyUtil.getBool("zstd_compression", true);
        int batchSize = SystemPropertyUtil.getInt("batch_size_per_request", 1000);
        int concurrency = SystemPropertyUtil.getInt("concurrency", 4);

        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        // Start a metrics exporter
        MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
        metricsExporter.init(ExporterOptions.newDefault());

        GreptimeDB greptimeDB = DBConnector.connect();

        Semaphore semaphore = new Semaphore(concurrency);

        try (TableDataProvider tableDataProvider = new MetricsTableDataProvider()) {
            tableDataProvider.init();
            TableSchema tableSchema = tableDataProvider.tableSchema();
            Iterator<Object[]> rows = tableDataProvider.rows();

            LOG.info(
                    "Start writing data, table: {}, row count: {}",
                    tableSchema.getTableName(),
                    tableDataProvider.rowCount());

            long millsOneDay = 1000 * 60 * 60 * 24;

            long start = System.nanoTime();
            AtomicLong totalRows = new AtomicLong(0);
            for (int i = 0; ; i++) {
                Table table = Table.from(tableSchema);

                for (int j = 0; j < batchSize; j++) {
                    if (!rows.hasNext()) {
                        break;
                    }
                    Object[] row = rows.next();

                    // Adjust timestamp to be 3-7 days ago for 10% of the data
                    if (i % 10 == 0) {
                        int days = ThreadLocalRandom.current().nextInt(3, 8);
                        long millis = millsOneDay * days;
                        row[0] = (long) row[0] - millis;
                    }

                    row[3] = i % 2;

                    table.addRow(row);
                }

                // Complete the table; adding rows is no longer permitted.
                table.complete();

                semaphore.acquire();

                // Write the table data to the server
                CompletableFuture<Result<WriteOk, Err>> future =
                        greptimeDB.write(Arrays.asList(table), WriteOp.Insert, ctx);
                future.whenComplete((result, error) -> {
                    semaphore.release();

                    if (error != null) {
                        LOG.error("Error writing data", error);
                        return;
                    }

                    int numRows = result.mapOr(0, writeOk -> writeOk.getSuccess());
                    totalRows.addAndGet(numRows);
                });

                if (i % 100 == 0) {
                    LOG.info("Wrote {} rows", totalRows.get());
                }

                if (!rows.hasNext()) {
                    break;
                }
            }

            // Wait for all the requests to complete
            semaphore.acquire(concurrency);

            LOG.info("Completed writing data, time cost: {}s", (System.nanoTime() - start) / 1000000000);

        } finally {
            greptimeDB.shutdownGracefully();
            metricsExporter.shutdownGracefully();
        }
    }
}
