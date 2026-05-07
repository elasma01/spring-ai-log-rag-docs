package com.bmax.lograg.service;

import com.bmax.lograg.model.AppLog;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

/**
 * Génère des logs Spring Boot réalistes avec des patterns variés :
 * requêtes HTTP, opérations BDD, authentification, cache, Kafka, circuit breaker.
 *
 * Distribution des niveaux : INFO 60% | WARN 20% | ERROR 15% | DEBUG 5%
 */
@Service
public class LogGeneratorService {

    private static final Random RANDOM = new Random();

    private static final String[] SERVICES = {
        "UserService", "OrderService", "PaymentService", "AuthService",
        "ProductService", "CartService", "NotificationService",
        "InventoryService", "ReportService", "EmailService"
    };

    private static final Map<String, String> SERVICE_LOGGERS = Map.of(
        "UserService",        "c.b.l.service.UserService",
        "OrderService",       "c.b.l.service.OrderService",
        "PaymentService",     "c.b.l.service.PaymentService",
        "AuthService",        "c.b.l.service.AuthService",
        "ProductService",     "c.b.l.service.ProductService",
        "CartService",        "c.b.l.service.CartService",
        "NotificationService","c.b.l.service.NotificationService",
        "InventoryService",   "c.b.l.service.InventoryService",
        "ReportService",      "c.b.l.service.ReportService",
        "EmailService",       "c.b.l.service.EmailService"
    );

    private static final String[] THREADS = {
        "http-nio-8080-exec-1", "http-nio-8080-exec-2", "http-nio-8080-exec-3",
        "http-nio-8080-exec-4", "http-nio-8080-exec-5",
        "scheduling-1", "kafka-consumer-1", "kafka-consumer-2",
        "async-executor-1", "async-executor-2", "main"
    };

    private static final String[] HTTP_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH"};

    private static final String[] HTTP_PATHS = {
        "/api/users", "/api/users/{id}", "/api/users/{id}/profile",
        "/api/orders", "/api/orders/{id}", "/api/orders/{id}/status",
        "/api/products", "/api/products/{id}",
        "/api/auth/login", "/api/auth/logout", "/api/auth/refresh",
        "/api/cart", "/api/cart/items/{id}",
        "/api/payments/{id}", "/api/inventory/{productId}",
        "/api/reports/daily", "/api/notifications"
    };

    private static final String[] USERS = {
        "john.doe@example.com", "jane.smith@example.com", "bob.wilson@example.com",
        "alice.martin@example.com", "charlie.brown@example.com",
        "david.jones@example.com", "emma.davis@example.com", "frank.miller@example.com"
    };

    private static final String[] IPS = {
        "192.168.1.100", "192.168.1.101", "10.0.0.15",
        "172.16.0.5", "192.168.2.50", "10.10.10.20", "203.0.113.42"
    };

    private static final String[] KAFKA_TOPICS = {
        "order-events", "payment-events", "user-events",
        "inventory-events", "notification-events"
    };

    // Distribution pondérée : INFO=60%, WARN=20%, ERROR=15%, DEBUG=5%
    private static final String[] WEIGHTED_LEVELS = buildWeightedLevels();

    private static String[] buildWeightedLevels() {
        List<String> levels = new ArrayList<>();
        for (int i = 0; i < 60; i++) levels.add("INFO");
        for (int i = 0; i < 20; i++) levels.add("WARN");
        for (int i = 0; i < 15; i++) levels.add("ERROR");
        for (int i = 0; i < 5;  i++) levels.add("DEBUG");
        return levels.toArray(new String[0]);
    }

    public AppLog generate() {
        String service = randomFrom(SERVICES);
        String level   = randomFrom(WEIGHTED_LEVELS);
        LogMessage lm  = buildMessage(service, level);

        return AppLog.builder()
            .id(UUID.randomUUID().toString())
            .timestamp(Instant.now())
            .level(level)
            .service(service)
            .thread(randomFrom(THREADS))
            .logger(SERVICE_LOGGERS.get(service))
            .message(lm.message())
            .exception(lm.exception())
            .traceId("trace-" + UUID.randomUUID().toString().substring(0, 8))
            .environment("production")
            .build();
    }

    // ─── Builders par niveau ──────────────────────────────────────────────────

    private LogMessage buildMessage(String service, String level) {
        return switch (level) {
            case "INFO"  -> buildInfo(service);
            case "WARN"  -> buildWarn(service);
            case "ERROR" -> buildError(service);
            case "DEBUG" -> buildDebug(service);
            default      -> new LogMessage("Application heartbeat OK", null);
        };
    }

    private LogMessage buildInfo(String service) {
        long   id       = rand(1, 99999);
        int    duration = rand(5, 250);
        String method   = randomFrom(HTTP_METHODS);
        String path     = resolvePath(randomFrom(HTTP_PATHS), id);
        String user     = randomFrom(USERS);

        String msg = switch (rand(0, 6)) {
            case 0 -> "Request completed: %s %s — status=%d — duration=%dms — ip=%s"
                       .formatted(method, path, pickStatus(), duration, randomFrom(IPS));
            case 1 -> "User authenticated successfully: %s — ip=%s — sessionId=%s"
                       .formatted(user, randomFrom(IPS), shortUUID());
            case 2 -> "Query executed: SELECT * FROM %s WHERE id=%d — rows=%d — duration=%dms"
                       .formatted(tableName(service), id, rand(1, 50), duration);
            case 3 -> "Cache HIT for key: %s:%d — TTL=%ds remaining"
                       .formatted(service.toLowerCase(), id, rand(60, 3600));
            case 4 -> "Kafka message published to topic: %s — partition=%d — offset=%d"
                       .formatted(randomFrom(KAFKA_TOPICS), rand(0, 3), rand(1000, 999999));
            case 5 -> "Scheduled task [%s-sync] completed — processed=%d records — duration=%dms"
                       .formatted(service, rand(10, 5000), rand(100, 5000));
            default -> "Transaction committed for %s.save(id=%d) — duration=%dms"
                       .formatted(service, id, duration);
        };
        return new LogMessage(msg, null);
    }

    private LogMessage buildWarn(String service) {
        long id = rand(1, 99999);

        String msg = switch (rand(0, 5)) {
            case 0 -> "Slow query detected: SELECT * FROM %s — duration=%dms — threshold=500ms"
                       .formatted(tableName(service), rand(500, 4000));
            case 1 -> "Cache miss rate high: %.1f%% — consider increasing maxSize for %s"
                       .formatted(40 + RANDOM.nextDouble() * 30, service);
            case 2 -> "Retry attempt %d/3 for %s on entity id=%d — cause: connection reset"
                       .formatted(rand(1, 3), service, id);
            case 3 -> "Database connection pool near capacity: %d/%d active connections"
                       .formatted(rand(17, 19), 20);
            default -> "JWT token expiring soon for user: %s — refresh required in %ds"
                       .formatted(randomFrom(USERS), rand(30, 300));
        };
        return new LogMessage(msg, null);
    }

    private LogMessage buildError(String service) {
        long   id      = rand(1, 99999);
        String orderId = "ORD-" + rand(10000, 99999);
        String user    = randomFrom(USERS);

        return switch (rand(0, 6)) {
            case 0 -> new LogMessage(
                "Authentication FAILED for user: %s from ip=%s — Reason: Invalid credentials"
                    .formatted(user, randomFrom(IPS)),
                """
                org.springframework.security.authentication.BadCredentialsException: Invalid credentials
                    at c.b.l.service.AuthService.authenticate(AuthService.java:67)
                    at c.b.l.controller.AuthController.login(AuthController.java:34)
                    at o.s.web.servlet.FrameworkServlet.service(FrameworkServlet.java:897)"""
            );
            case 1 -> new LogMessage(
                "Payment processing FAILED for %s — Gateway timeout after 30000ms".formatted(orderId),
                """
                java.util.concurrent.TimeoutException: Stripe API did not respond within 30000ms
                    at c.b.l.service.PaymentService.processPayment(PaymentService.java:89)
                    at c.b.l.service.OrderService.checkout(OrderService.java:156)
                Caused by: java.net.SocketTimeoutException: Read timed out"""
            );
            case 2 -> new LogMessage(
                "NullPointerException in %s while processing entityId=%d".formatted(service, id),
                """
                java.lang.NullPointerException: Cannot invoke getId() because entity is null
                    at c.b.l.service.%s.processEntity(%s.java:%d)
                    at c.b.l.service.%s.handleRequest(%s.java:%d)"""
                    .formatted(service, service, rand(50, 200), service, service, rand(20, 100))
            );
            case 3 -> new LogMessage(
                "DataIntegrityViolationException: INSERT INTO %s — duplicate key constraint violated"
                    .formatted(tableName(service)),
                """
                org.springframework.dao.DataIntegrityViolationException: could not execute statement
                    at o.s.orm.jpa.vendor.HibernateJpaDialect.convertHibernateAccessException(HibernateJpaDialect.java:321)
                Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint "users_email_key"
                    at o.p.Driver.connectImpl(Driver.java:497)"""
            );
            case 4 -> new LogMessage(
                "Circuit breaker '%s' OPENED — failure rate %.0f%% exceeds threshold 50%%"
                    .formatted(service, 50 + RANDOM.nextDouble() * 40),
                null
            );
            case 5 -> new LogMessage(
                "OutOfMemoryError in %s — Java heap space exhausted during batch processing"
                    .formatted(service),
                """
                java.lang.OutOfMemoryError: Java heap space
                    at java.util.Arrays.copyOf(Arrays.java:3213)
                    at c.b.l.service.%s.loadBatch(%s.java:%d)"""
                    .formatted(service, service, rand(80, 300))
            );
            default -> new LogMessage(
                "Unhandled exception in %s REST endpoint — correlationId=%s".formatted(service, shortUUID()),
                """
                java.lang.RuntimeException: Unexpected error during request processing
                    at c.b.l.controller.GlobalExceptionHandler.handleException(GlobalExceptionHandler.java:45)
                    at o.s.web.servlet.DispatcherServlet.processDispatchResult(DispatcherServlet.java:1076)"""
            );
        };
    }

    private LogMessage buildDebug(String service) {
        long id = rand(1, 99999);
        String msg = switch (rand(0, 4)) {
            case 0 -> "Entering %s.findById(id=%d) — cache lookup initiated".formatted(service, id);
            case 1 -> "Bean proxy resolved: %s → %sImpl — scope=singleton".formatted(service, service);
            case 2 -> "Transaction started for %s.save() — propagation=REQUIRED, isolation=DEFAULT".formatted(service);
            default -> "HTTP response headers set: Content-Type=application/json, X-Trace-Id=%s".formatted(shortUUID());
        };
        return new LogMessage(msg, null);
    }

    // ─── Utilitaires ──────────────────────────────────────────────────────────

    private int pickStatus() {
        int r = rand(0, 100);
        if (r < 70) return 200;
        if (r < 80) return 201;
        if (r < 85) return 400;
        if (r < 90) return 401;
        if (r < 95) return 404;
        return 500;
    }

    private String tableName(String service) {
        return service.toLowerCase().replace("service", "") + "s";
    }

    private String resolvePath(String path, long id) {
        return path.replace("{id}", String.valueOf(id))
                   .replace("{productId}", String.valueOf(rand(1, 999)));
    }

    private String shortUUID() {
        return UUID.randomUUID().toString().substring(0, 12);
    }

    private int rand(int min, int max) {
        return RANDOM.nextInt(max - min) + min;
    }

    @SuppressWarnings("unchecked")
    private <T> T randomFrom(T[] array) {
        return array[RANDOM.nextInt(array.length)];
    }

    record LogMessage(String message, String exception) {}
}
