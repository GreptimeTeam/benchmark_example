package io.greptime.bench.benchmark;

import io.greptime.common.util.SystemPropertyUtil;

public class Benchmark {
    public static void main(String[] args) throws Exception {
        String target = SystemPropertyUtil.get("target", "metrics");
        if (target.equalsIgnoreCase("metrics")) {
            MetricsBenchmark.main(args);
        } else if (target.equalsIgnoreCase("logs")) {
            LogsBenchmark.main(args);
        } else if (target.equalsIgnoreCase("traces")) {
            TracesBenchmark.main(args);
        } else if (target.equalsIgnoreCase("bulk_metrics")) {
            BulkMetricsBenchmark.main(args);
        } else {
            throw new IllegalArgumentException("Invalid target: " + target);
        }
    }
}
