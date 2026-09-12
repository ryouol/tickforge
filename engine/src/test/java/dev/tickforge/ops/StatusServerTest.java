package dev.tickforge.ops;

import static org.junit.jupiter.api.Assertions.*;

import java.net.*;
import java.net.http.*;
import org.junit.jupiter.api.Test;

class StatusServerTest {
  @Test
  void readinessAndExplicitCommittedPublication() throws Exception {
    try (var server = new StatusServer(0, new Metrics());
        var client = HttpClient.newHttpClient()) {
      String base = "http://127.0.0.1:" + server.port();
      assertEquals(
          503,
          client
              .send(
                  HttpRequest.newBuilder(URI.create(base + "/health/ready")).build(),
                  HttpResponse.BodyHandlers.ofString())
              .statusCode());
      server.publish("{\"cash\":123}");
      server.ready.set(true);
      assertEquals(
          "{\"cash\":123}",
          client
              .send(
                  HttpRequest.newBuilder(URI.create(base + "/status")).build(),
                  HttpResponse.BodyHandlers.ofString())
              .body());
      assertEquals(
          200,
          client
              .send(
                  HttpRequest.newBuilder(URI.create(base + "/health/ready")).build(),
                  HttpResponse.BodyHandlers.ofString())
              .statusCode());
    }
  }
}
