package dev.tickforge.ops;

import dev.tickforge.engine.EngineState;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.HdrHistogram.ConcurrentHistogram;

public final class Metrics {
  public final AtomicLong parsed = new AtomicLong(),
      committed = new AtomicLong(),
      blockedNs = new AtomicLong(),
      commitFailures = new AtomicLong(),
      queueDepth = new AtomicLong(),
      recoveries = new AtomicLong(),
      processedIndex = new AtomicLong();
  private final AtomicLong highWatermark = new AtomicLong(),
      admissions = new AtomicLong(),
      firstAdmission = new AtomicLong(),
      lastAdmission = new AtomicLong(),
      lastCommittedIndex = new AtomicLong(),
      duplicates = new AtomicLong(),
      quarantined = new AtomicLong(),
      rejectedOrders = new AtomicLong(),
      fills = new AtomicLong();
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

  public void observeDepth(int depth) {
    queueDepth.set(depth);
    highWatermark.accumulateAndGet(depth, Math::max);
  }

  public void admitted(long time) {
    if (admissions.getAndIncrement() == 0) firstAdmission.set(time);
    lastAdmission.set(time);
  }

  public void committedState(EngineState state) {
    lastCommittedIndex.set(state.nextIndex - 1);
    duplicates.set(state.duplicates);
    quarantined.set(state.quarantined);
    rejectedOrders.set(state.rejections);
    fills.set(state.fillCount);
  }

  public Map<String, Object> snapshot() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("parsed", parsed.get());
    result.put("committed", committed.get());
    result.put("producerBlockedNs", blockedNs.get());
    result.put("commitFailures", commitFailures.get());
    result.put("queueDepth", queueDepth.get());
    result.put("queueHighWatermark", highWatermark.get());
    result.put("recoveries", recoveries.get());
    result.put("processedIndex", processedIndex.get());
    result.put("lastCommittedIndex", lastCommittedIndex.get());
    result.put("duplicates", duplicates.get());
    result.put("quarantined", quarantined.get());
    result.put("rejectedOrders", rejectedOrders.get());
    result.put("fills", fills.get());
    long span = lastAdmission.get() - firstAdmission.get();
    result.put("actualOfferedPerSecond", span > 0 ? (admissions.get() - 1) * 1e9 / span : 0.0);
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
            (name, value) -> {
              if (value instanceof Map<?, ?> statistics)
                statistics.forEach(
                    (statistic, number) ->
                        b.append("tickforge_")
                            .append(name)
                            .append('_')
                            .append(statistic)
                            .append(' ')
                            .append(number)
                            .append('\n'));
              else b.append("tickforge_").append(name).append(' ').append(value).append('\n');
            });
    return b.toString();
  }
}
