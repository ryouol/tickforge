package dev.tickforge.ops;

import com.sun.net.httpserver.HttpServer;
import dev.tickforge.io.Json;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class StatusServer implements AutoCloseable {
  private final HttpServer server;
  private final ExecutorService executor = Executors.newFixedThreadPool(2);
  public final AtomicBoolean ready = new AtomicBoolean();
  private final AtomicReference<String> committed = new AtomicReference<>("{}");

  public StatusServer(int port, Metrics metrics) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
    server.setExecutor(executor);
    server.createContext(
        "/",
        exchange -> {
          String path = exchange.getRequestURI().getPath();
          int code = 200;
          String body;
          String type = "application/json";
          switch (path) {
            case "/health/live" -> body = "{\"live\":true}";
            case "/health/ready" -> {
              code = ready.get() ? 200 : 503;
              body = Json.encode(Map.of("ready", ready.get()));
            }
            case "/status" -> body = committed.get();
            case "/metrics" -> {
              body = metrics.prometheus();
              type = "text/plain; version=0.0.4";
            }
            case "/metrics.json" -> body = Json.encode(metrics.snapshot());
            default -> {
              code = 404;
              body = "{}";
            }
          }
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", type);
          exchange.sendResponseHeaders(code, bytes.length);
          try (var out = exchange.getResponseBody()) {
            out.write(bytes);
          } finally {
            exchange.close();
          }
        });
    server.start();
  }

  public void publish(String json) {
    committed.set(json);
  }

  public int port() {
    return server.getAddress().getPort();
  }

  @Override
  public void close() {
    ready.set(false);
    server.stop(0);
    executor.shutdownNow();
  }
}
