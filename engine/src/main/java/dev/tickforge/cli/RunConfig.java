package dev.tickforge.cli;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import dev.tickforge.io.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public record RunConfig(
    long initialCash,
    long feePerShare,
    int orderQuantity,
    int maxOrderQuantity,
    int maxPosition,
    long quoteFreshnessNs,
    long latencyNs,
    long timeoutNs,
    long buySlippageTicks,
    boolean quarantineMalformed,
    boolean tolerateGaps,
    Map<String, Threshold> symbols) {
  public record Threshold(long entry, long exit) {}

  public RunConfig {
    if (initialCash < 0
        || feePerShare < 0
        || orderQuantity <= 0
        || maxOrderQuantity <= 0
        || maxPosition <= 0
        || quoteFreshnessNs < 0
        || latencyNs < 0
        || timeoutNs <= latencyNs
        || buySlippageTicks < 0
        || symbols == null
        || symbols.isEmpty()) throw new IllegalArgumentException("invalid risk configuration");
    symbols = Collections.unmodifiableMap(new TreeMap<>(symbols));
    symbols.forEach(
        (s, t) -> {
          if (!s.matches("[A-Z][A-Z0-9_]{0,31}") || t == null || t.entry <= 0 || t.entry >= t.exit)
            throw new IllegalArgumentException("invalid symbol thresholds");
        });
  }

  public static RunConfig load(Path path) throws IOException {
    return new YAMLMapper().readValue(path.toFile(), RunConfig.class);
  }

  public String hash() {
    return DatasetManifest.hashText(Json.encode(this));
  }
}
