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
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        int numTables = SystemPropertyUtil.getInt("num_log_tables", 1);

        LOG.info("Using zstd compression: {}", zstdCompression);
        LOG.info("Batch size: {}", batchSize);
        LOG.info("Concurrency: {}", concurrency);
        LOG.info("Number of tables: {}", numTables);

        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        // Start a metrics exporter
        MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
        metricsExporter.init(ExporterOptions.newDefault());

        GreptimeDB greptimeDB = DBConnector.connect();

        Semaphore semaphore = new Semaphore(concurrency);
        ExecutorService executorService = Executors.newFixedThreadPool(numTables);

        // Create table data providers for each table
        List<TableDataProvider> tableDataProviders = new ArrayList<>();
        for (int i = 0; i < numTables; i++) {
            String tableName = "tt_log_table_" + i;
            TableDataProvider tableDataProvider = new LogTableDataProvider(tableName);
            tableDataProviders.add(tableDataProvider);
        }
        LOG.info("Created {} table data providers", numTables);
        
        try {
            // Initialize all table data providers
            for (TableDataProvider provider : tableDataProviders) {
                provider.init();
            }

            LOG.info("Start writing data to {} tables in parallel", numTables);

            long start = System.nanoTime();
            AtomicLong totalRows = new AtomicLong(0);
            
            // Submit tasks for each table to run in parallel
            List<CompletableFuture<Void>> tableTasks = new ArrayList<>();
            
            for (int tableIndex = 0; tableIndex < numTables; tableIndex++) {
                final int finalTableIndex = tableIndex;
                TableDataProvider tableDataProvider = tableDataProviders.get(tableIndex);
                
                CompletableFuture<Void> tableTask = CompletableFuture.runAsync(() -> {
                    try {
                        TableSchema tableSchema = tableDataProvider.tableSchema();
                        Iterator<Object[]> rows = tableDataProvider.rows();
                        String tableName = tableSchema.getTableName();
                        
                        LOG.info("Starting ingestion for table: {}", tableName);
                        
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
                                    LOG.error("Error writing data to {}, time cost: {}ms", tableName, costMs, error);
                                    return;
                                }

                                int numRows = result.mapOr(0, writeOk -> writeOk.getSuccess());
                                totalRows.addAndGet(numRows);
                                LOG.info("Wrote {} rows to {}, time cost: {}ms", numRows, tableName, costMs);
                            });

                            if (i % 100 == 0) {
                                LOG.info("Table {} - batches processed: {}", tableName, i);
                            }

                            if (!rows.hasNext()) {
                                LOG.info("Completed ingestion for table: {}", tableName);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error in table task for table index {}", finalTableIndex, e);
                    }
                }, executorService);
                
                tableTasks.add(tableTask);
            }
            
            // Wait for all table tasks to complete
            CompletableFuture.allOf(tableTasks.toArray(new CompletableFuture[0])).join();
            
            // Wait for all the requests to complete
            semaphore.acquire(concurrency);

            LOG.info("Completed writing data to all {} tables, total rows: {}, time cost: {}s", 
                    numTables, totalRows.get(), (System.nanoTime() - start) / 1000000000);

        } finally {
            // Close all table data providers
            for (TableDataProvider provider : tableDataProviders) {
                provider.close();
            }
            executorService.shutdown();
            greptimeDB.shutdownGracefully();
            metricsExporter.shutdownGracefully();
        }
    }
}