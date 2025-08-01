package io.greptime.bench;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Helper class for generating log messages.
 */
public class LogTextHelper {

    private static final String[] LOG_LEVELS = {"INFO", "WARN", "ERROR", "DEBUG"};

    // Log templates as static constants to avoid recreating them for each call
    private static final String[] LOG_TEMPLATES = {
        "INFO: Request processed successfully. RequestID: %s, Duration: %dms, UserID: %s",
        "WARN: Slow query detected. QueryID: %s, Duration: %dms, SQL: %s",
        "ERROR: Failed to connect to database. Attempt: %d, Error: %s, Host: %s",
        "DEBUG: Cache hit ratio: %d%%, Keys: %d, Misses: %d, Memory usage: %dMB",
        "INFO: User authentication successful. UserID: %s, IP: %s, LoginTime: %s"
    };

    // Stack trace frames as static constants
    private static final String[] STACK_FRAMES = {
        "at io.greptime.service.DatabaseConnector.connect(DatabaseConnector.java:142)",
        "at io.greptime.service.QueryExecutor.execute(QueryExecutor.java:85)",
        "at io.greptime.api.RequestHandler.processQuery(RequestHandler.java:213)",
        "at io.greptime.server.GrpcService.handleRequest(GrpcService.java:178)",
        "at io.greptime.server.HttpEndpoint.doPost(HttpEndpoint.java:95)",
        "at javax.servlet.http.HttpServlet.service(HttpServlet.java:707)",
        "at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:227)"
    };

    // Context keys as static constants
    private static final String[] CONTEXT_KEYS = {
        "client_version", "server_version", "cluster_id", "node_id", "region", "datacenter"
    };

    // Pre-computed stack trace and context strings to avoid concatenation in hot path
    private static final String STACK_TRACE_PREFIX = "\nStack trace:\n";
    private static final String CONTEXT_PREFIX = "\nAdditional context: ";
    private static final String CONTEXT_SEPARATOR = ", ";
    private static final String CONTEXT_EQUALS = "=";

    public static String randomLogLevel(ThreadLocalRandom random) {
        return LOG_LEVELS[random.nextInt(LOG_LEVELS.length)];
    }

    public static String generateTextWihtLen(ThreadLocalRandom random, long logTs, int length) {
        StringBuilder buf = new StringBuilder(length);

        // Choose template based on log level distribution
        int r = random.nextInt(100);
        String template;
        if (r < 1) { // 1% ERROR
            template = LOG_TEMPLATES[2]; // ERROR template
        } else if (r < 6) { // 5% WARN
            template = LOG_TEMPLATES[1]; // WARN template
        } else if (r < 16) { // 10% DEBUG
            template = LOG_TEMPLATES[3]; // DEBUG template
        } else { // 84% INFO
            template = random.nextBoolean()
                    ? LOG_TEMPLATES[0]
                    : LOG_TEMPLATES[4]; // Randomly choose between INFO templates
        }

        // Format the template with the prepared values
        String formattedLog;
        if (template.startsWith("INFO: Request")) {
            String requestId = "req" + random.nextLong(1000000);
            int duration = random.nextInt(1, 5000);
            String userId = "user" + random.nextInt(10000);
            formattedLog = String.format(template, requestId, duration, userId);
        } else if (template.startsWith("WARN:")) {
            String queryId = "query" + random.nextInt(50000);
            int duration = random.nextInt(1, 5000);
            formattedLog = String.format(template, queryId, duration, "SELECT * FROM table" + random.nextInt(100));
        } else if (template.startsWith("ERROR:")) {
            formattedLog = String.format(template, random.nextInt(5), "Connection refused", "db" + random.nextInt(10));
        } else if (template.startsWith("DEBUG:")) {
            int count = random.nextInt(10000);
            int percentage = random.nextInt(100);
            formattedLog = String.format(template, percentage, count, random.nextInt(1000), random.nextInt(512));
        } else { // Second INFO template
            String userId = "user" + random.nextInt(10000);
            formattedLog = String.format(
                    template, userId, "192.168." + random.nextInt(256) + "." + random.nextInt(256), logTs);
        }

        buf.append(formattedLog);

        int targetLength = random.nextInt(length - 500, length);

        // Add stack trace for error logs to reach desired size
        if (formattedLog.startsWith("ERROR")) {
            buf.append(STACK_TRACE_PREFIX);

            // Add stack frames until we reach target length
            int frameIndex = 0;
            int framesCount = STACK_FRAMES.length;

            while (buf.length() < targetLength && frameIndex < 100) { // Limit iterations
                buf.append(STACK_FRAMES[frameIndex % framesCount]).append('\n');
                frameIndex++;
            }
        } else {
            // For non-error logs, pad with additional context information
            buf.append(CONTEXT_PREFIX);

            // Add key-value pairs until we reach desired length
            int keyIndex = 0;
            int keysCount = CONTEXT_KEYS.length;

            while (buf.length() < targetLength && keyIndex < 100) { // Limit iterations
                buf.append(CONTEXT_KEYS[keyIndex % keysCount])
                        .append(CONTEXT_EQUALS)
                        .append(random.nextInt(1000))
                        .append(CONTEXT_SEPARATOR);
                keyIndex++;
            }
        }

        return buf.toString();
    }
}
