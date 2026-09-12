package dev.tickforge.ops;

import java.util.*;
import java.util.concurrent.atomic.*;
import org.HdrHistogram.ConcurrentHistogram;

public final class Metrics {
  public final AtomicLong parsed = new AtomicLong(),
      committed = new AtomicLong(),
      blockedNs = new AtomicLong(),
      commitFailures = new AtomicLong(),
      queueDepth = new AtomicLong(),
      recoveries = new AtomicLong();
  private final Map<String, ConcurrentHistogram> histograms = new LinkedHashMap<>();

  public Metrics() {
    for (String name :
        List.of(
            "queue_wait",
            "engine",
            "commit",
            "admission_to_commit",
            "scheduled_to_commit",
            "schedule_lateness")) histograms.put(name, new ConcurrentHistogram(3600000000000L, 3));
  }

  public void record(String name, long nanos) {
    histograms.get(name).recordValue(Math.max(0, Math.min(nanos, 3600000000000L)));
  }

  public Map<String, Object> snapshot() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("parsed", parsed.get());
    result.put("committed", committed.get());
    result.put("producerBlockedNs", blockedNs.get());
    result.put("commitFailures", commitFailures.get());
    result.put("queueDepth", queueDepth.get());
    result.put("recoveries", recoveries.get());
    histograms.forEach(
        (name, h) -> {
          var copy = h.copy();
          result.put(
              name + "Ns",
              Map.of(
                  "count",
                  copy.getTotalCount(),
                  "p50",
                  copy.getValueAtPercentile(50),
                  "p95",
                  copy.getValueAtPercentile(95),
                  "p99",
                  copy.getValueAtPercentile(99),
                  "max",
                  copy.getMaxValue()));
        });
    return result;
  }

  public String prometheus() {
    StringBuilder b = new StringBuilder();
    snapshot()
        .forEach(
            (k, v) -> {
              if (v instanceof Number)
                b.append("tickforge_").append(k).append(' ').append(v).append('\n');
            });
    return b.toString();
  }
}
