package io.greptime.bench.benchmark;

import io.greptime.BulkStreamWriter;
import io.greptime.BulkWrite;
import io.greptime.GreptimeDB;
import io.greptime.bench.BulkMetricsTableDataProvider;
import io.greptime.bench.DBConnector;
import io.greptime.bench.TableDataProvider;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.metrics.ExporterOptions;
import io.greptime.metrics.MetricsExporter;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkMetricsBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(BulkMetricsBenchmark.class);

    public static void main(String[] args) throws Exception {
        boolean zstdCompression = SystemPropertyUtil.getBool("zstd_compression", true);
        int batchSize = SystemPropertyUtil.getInt("batch_size_per_request", 10_0000);
        int maxRequestsInFlight = SystemPropertyUtil.getInt("max_requests_in_flight", 4);
        int concurrency = SystemPropertyUtil.getInt("concurrency", 4);

        LOG.info("Using zstd compression: {}", zstdCompression);
        LOG.info("Batch size: {}", batchSize);
        LOG.info("Max requests in flight: {}", maxRequestsInFlight);
        LOG.info("Concurrency: {}", concurrency);

        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        // Start a metrics exporter
        MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
        metricsExporter.init(ExporterOptions.newDefault());

        GreptimeDB greptimeDB = DBConnector.connect();
        BulkWrite.Config cfg = BulkWrite.Config.newBuilder()
                .allocatorInitReservation(0)
                .allocatorMaxAllocation(4 * 1024 * 1024 * 1024L)
                .timeoutMsPerMessage(60000)
                .maxRequestsInFlight(maxRequestsInFlight)
                .build();

        TableDataProvider tableDataProvider = new BulkMetricsTableDataProvider();
        LOG.info("Table data provider: {}", tableDataProvider.getClass().getName());
        tableDataProvider.init();
        TableSchema tableSchema = tableDataProvider.tableSchema();

        Semaphore semaphore = new Semaphore(concurrency);
        int shard = 0;
        int requestCount = 1;
        long millsOneDay = 1000 * 60 * 60 * 24;
        AtomicLong totalRowsWritten = new AtomicLong(0);

        LOG.info("Start writing data");
        try (BulkStreamWriter writer = greptimeDB.bulkStreamWriter(tableSchema, cfg, ctx)) {
            Iterator<Object[]> rows = tableDataProvider.rows();

            long start = System.nanoTime();
            do {
                Table.TableBufferRoot table = writer.tableBufferRoot(10_0000);
                int days = ThreadLocalRandom.current().nextInt(3, 8);
                for (int i = 0; i < batchSize; i++) {
                    if (!rows.hasNext()) {
                        break;
                    }
                    Object[] row = rows.next();

                    // Adjust timestamp to be 3-7 days ago for 10% of the data
                    if (requestCount % 10 == 0) {
                        long millis = millsOneDay * days;
                        row[0] = (long) row[0] - millis;
                    }

                    row[3] = shard % 2;

                    table.addRow(row);
                }
                LOG.info("Table bytes used: {}", table.bytesUsed());
                // Complete the table; adding rows is no longer permitted.
                table.complete();

                semaphore.acquire();

                // Write the table data to the server
                CompletableFuture<Integer> future = writer.writeNext();
                long fStart = System.nanoTime();
                future.whenComplete((r, t) -> {
                    semaphore.release();

                    long costMs = (System.nanoTime() - fStart) / 1000000;
                    if (t != null) {
                        LOG.error("Error writing data, time cost: {}ms", costMs, t);
                        return;
                    }

                    long totalRows = totalRowsWritten.addAndGet(r);
                    long totalElapsedSec = (System.nanoTime() - start) / 1000000000;
                    long writeRatePerSecond = totalElapsedSec > 0 ? totalRows / totalElapsedSec : 0;
                    LOG.info(
                            "Wrote rows: {}, time cost: {}ms, total rows: {}, total elapsed: {}s, write rate: {} rows/sec",
                            r,
                            costMs,
                            totalRows,
                            totalElapsedSec,
                            writeRatePerSecond);
                });

                shard++;
                requestCount++;
            } while (rows.hasNext());

            writer.completed();

            // Wait for all the requests to complete
            semaphore.acquire(concurrency);

            LOG.info("Completed writing data, time cost: {}s", (System.nanoTime() - start) / 1000000000);
        } finally {
            tableDataProvider.close();
        }

        greptimeDB.shutdownGracefully();
        metricsExporter.shutdownGracefully();
    }
}
