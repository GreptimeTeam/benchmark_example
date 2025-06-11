# Benchmark Example - Fat JAR

这个项目可以使用 Maven Shade Plugin 将 Java 项目打包为一个包含所有依赖的 fat jar（也称为 uber jar）。

## 构建 Fat JAR

### 方法一：使用构建脚本
```bash
./build-fat-jar.sh
```

### 方法二：使用 Maven 命令
```bash
mvn clean package
```

## 运行应用

构建完成后，你会在 `target/` 目录下看到 `benchmark_example.jar` 文件（大约 45MB）。

直接运行：
```bash
java -jar target/benchmark_example.jar
```

## 基准测试类型

该程序支持三种不同的基准测试类型，通过 `target` 系统属性指定：

### 1. Metrics 基准测试（默认）
```bash
java -Dtarget=metrics -jar target/benchmark_example.jar
```

### 2. Logs 基准测试
```bash
java -Dtarget=logs -jar target/benchmark_example.jar
```

### 3. Traces 基准测试
```bash
java -Dtarget=traces -jar target/benchmark_example.jar
```

### 4. Bulk Metrics 基准测试
```bash
java -Dtarget=bulk_metrics -jar target/benchmark_example.jar
```

## 配置参数

### 通用参数
- `target`: 基准测试类型（metrics/logs/traces，默认：metrics）
- `table_row_count`: 一共写入多少条数据，写完程序停止运行（默认：Metrics 50 亿行，Logs/Traces 1 亿行）
- `zstd_compression`: 是否启用 ZSTD 压缩（默认：true）
- `batch_size_per_request`: 每次请求的批次大小（Metrics 默认：1000，Logs 默认：64 * 1024, Traces 默认：20 * 1024, Bulk Metrics 默认：10 * 1000）

### Metrics 基准测试特有参数
- `concurrency`: 并发数（默认：4）
- `tt_metrics_table.service_num_per_app`: 每个应用的服务数量（默认：20）

### Logs/Traces/Bulk Metrics 基准测试特有参数
- `max_requests_in_flight`: 最大飞行中请求数（默认：4）

## 使用示例

### 运行 Metrics 基准测试
```bash
java -Dtarget=metrics -Dtable_row_count=1000000 -Dzstd_compression=false -Dbatch_size_per_request=500 -Dconcurrency=4 -jar target/benchmark_example.jar
```

### 运行 Logs 基准测试
```bash
java -Dtarget=logs -Dtable_row_count=1000000 -Dbatch_size_per_request=32768 -jar target/benchmark_example.jar
```

### 运行 Traces 基准测试
```bash
java -Dtarget=traces -Dtable_row_count=1000000 -Dbatch_size_per_request=32768 -jar target/benchmark_example.jar
```

### 运行 Bulk Metrics 基准测试
```bash
java -Dtarget=bulk_metrics -Dtable_row_count=1000000 -Dbatch_size_per_request=32768 -jar target/benchmark_example.jar
```
