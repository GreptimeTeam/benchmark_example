package io.greptime.bench.benchmark;

import io.greptime.GreptimeDB;
import io.greptime.WriteOp;
import io.greptime.bench.DBConnector;
import io.greptime.bench.LogTableDataProvider;
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
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogsNormalBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(LogsNormalBenchmark.class);

    public static void main(String[] args) throws Exception {
        boolean zstdCompression = SystemPropertyUtil.getBool("zstd_compression", true);
        int batchSize = SystemPropertyUtil.getInt("batch_size_per_request", 1000);
        int concurrency = SystemPropertyUtil.getInt("concurrency", 4);

        LOG.info("Using zstd compression: {}", zstdCompression);
        LOG.info("Batch size: {}", batchSize);
        LOG.info("Concurrency: {}", concurrency);

        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        // Start a metrics exporter
        MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
        metricsExporter.init(ExporterOptions.newDefault());

        GreptimeDB greptimeDB = DBConnector.connect();

        Semaphore semaphore = new Semaphore(concurrency);

        TableDataProvider tableDataProvider = new LogTableDataProvider();
        LOG.info("Table data provider: {}", tableDataProvider.getClass().getName());
        
        try {
            tableDataProvider.init();
            TableSchema tableSchema = tableDataProvider.tableSchema();
            Iterator<Object[]> rows = tableDataProvider.rows();

            LOG.info("Start writing data");

            long start = System.nanoTime();
            AtomicLong totalRows = new AtomicLong(0);
            for (int i = 0; ; i++) {
                Table table = Table.from(tableSchema);

                for (int j = 0; j < batchSize; j++) {
                    if (!rows.hasNext()) {
                        break;
                    }
                    table.addRow(rows.next());
                }

                // Complete the table; adding rows is no longer permitted.
                table.complete();

                semaphore.acquire();

                // Write the table data to the server
                CompletableFuture<Result<WriteOk, Err>> future =
                        greptimeDB.write(Arrays.asList(table), WriteOp.Insert, ctx);
                long fStart = System.nanoTime();
                future.whenComplete((result, error) -> {
                    long costMs = (System.nanoTime() - fStart) / 1000000;
                    semaphore.release();

                    if (error != null) {
                        LOG.error("Error writing data, time cost: {}ms", costMs, error);
                        return;
                    }

                    int numRows = result.mapOr(0, writeOk -> writeOk.getSuccess());
                    totalRows.addAndGet(numRows);
                    LOG.info("Wrote rows: {}, time cost: {}ms", numRows, costMs);
                });

                if (i % 100 == 0) {
                    LOG.info("Total rows written so far: {}", totalRows.get());
                }

                if (!rows.hasNext()) {
                    break;
                }
            }

            // Wait for all the requests to complete
            semaphore.acquire(concurrency);

            LOG.info("Completed writing data, time cost: {}s", (System.nanoTime() - start) / 1000000000);

        } finally {
            tableDataProvider.close();
            greptimeDB.shutdownGracefully();
            metricsExporter.shutdownGracefully();
        }
    }
}