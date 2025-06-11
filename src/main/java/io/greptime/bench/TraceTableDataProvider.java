package io.greptime.bench;

import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.models.DataType;
import io.greptime.models.TableSchema;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The trace table data provider.
 */
public class TraceTableDataProvider implements TableDataProvider {

    private final TableSchema tableSchema;
    private final long rowCount;

    /*
    ```sql
    CREATE TABLE IF NOT EXISTS `tt_trace_table` (
        `ts` TIMESTAMP(3) NOT NULL,
        `trace_id` STRING NULL SKIPPING INDEX,
        `span_id` STRING NULL SKIPPING INDEX,
        `serverity_text` STRING NULL,
        `serverity_number` BIGINT NULL,
        `trace_flags` INT NULL,
        `scope_name` STRING NULL,
        `scope_version` STRING NULL,
        `scope_attributes` STRING NULL,
        `scope_schema_url` STRING NULL,
        `resource_attributes` STRING NULL,
        `resource_schema_url` STRING NULL,
        `service_instance_id` BIGINT NULL SKIPPING INDEX,
        `service_id` BITINT NULL SKIPPING INDEX,
        `host_id` BIGINT NULL SKIPPING INDEX,
        `process_group_id` BIGINT NULL SKIPPING INDEX,
        `interface_id` BIGINT NULL SKIPPING INDEX,
        `app_id` BIGINT NULL SKIPPING INDEX,
        `process_id` BIGINT NULL SKIPPING INDEX,
        `app_request_id` BIGINT NULL SKIPPING INDEX,
        `detail` STRING NULL,
        TIME INDEX (`ts`)
    ) ENGINE=mito
    WITH(
      append_mode = 'true',
      skip_wal = 'true',
    );
    ```
     */

    {
        this.tableSchema = TableSchema.newBuilder("tt_trace_table")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("trace_id", DataType.String)
                .addField("span_id", DataType.String)
                .addField("serverity_text", DataType.String)
                .addField("serverity_number", DataType.Int64)
                .addField("trace_flags", DataType.Int32)
                .addField("scope_name", DataType.String)
                .addField("scope_version", DataType.String)
                .addField("scope_attributes", DataType.String)
                .addField("scope_schema_url", DataType.String)
                .addField("resource_attributes", DataType.String)
                .addField("resource_schema_url", DataType.String)
                .addField("service_instance_id", DataType.Int64)
                .addField("service_id", DataType.Int64)
                .addField("host_id", DataType.Int64)
                .addField("process_group_id", DataType.Int64)
                .addField("interface_id", DataType.Int64)
                .addField("app_id", DataType.Int64)
                .addField("process_id", DataType.Int64)
                .addField("app_request_id", DataType.Int64)
                .addField("detail", DataType.String)
                .build();
        this.rowCount = SystemPropertyUtil.getLong("table_row_count", 100_000_000L);
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
                String traceId = UUID.randomUUID().toString();
                String spanId = UUID.randomUUID().toString();
                String serverityText = "serverity_text_" + random.nextInt(10);
                long serverityNumber = random.nextLong(10);
                int traceFlags = random.nextInt(10);
                String scopeName = "scope_name_" + random.nextInt(10);
                String scopeVersion = "scope_version_" + random.nextInt(10);
                String scopeAttributes = "scope_attributes_" + random.nextInt(10);
                String scopeSchemaUrl = "scope_schema_url_" + random.nextInt(10);
                String resourceAttributes = "resource_attributes_" + random.nextInt(10);
                String resourceSchemaUrl = "resource_schema_url_" + random.nextInt(10);
                long serviceInstanceId = random.nextLong(100000);
                long serviceId = random.nextLong(10000);
                long hostId = random.nextLong(10000);
                long processGroupId = random.nextLong(100000);
                long interfaceId = random.nextLong(100000);
                long appId = random.nextLong(1000);
                long processId = random.nextLong(100000);
                long appRequestId = random.nextLong(100000);
                String detail = LogTextHelper.generateTextWihtLen(random, ts, 13000);

                return new Object[] {
                    ts,
                    traceId,
                    spanId,
                    serverityText,
                    serverityNumber,
                    traceFlags,
                    scopeName,
                    scopeVersion,
                    scopeAttributes,
                    scopeSchemaUrl,
                    resourceAttributes,
                    resourceSchemaUrl,
                    serviceInstanceId,
                    serviceId,
                    hostId,
                    processGroupId,
                    interfaceId,
                    appId,
                    processId,
                    appRequestId,
                    detail
                };
            }
        };
    }

    @Override
    public long rowCount() {
        return this.rowCount;
    }
}
