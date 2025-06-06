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

## 配置参数

该程序支持以下系统属性配置：
- `tt_metrics_table.row_count`: 一共写入多少条数据，写完程序停止运行，默认 100 亿行
- `zstd_compression`: 是否启用 ZSTD 压缩（默认：true）
- `batch_size_per_request`: 每次请求的批次大小（默认：1000）
- `concurrency`: 并发数（默认：8）

示例：
```bash
java -Dzstd_compression=false -Dbatch_size_per_request=500 -Dconcurrency=4 -jar target/benchmark_example.jar
```
