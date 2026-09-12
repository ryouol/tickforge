package dev.tickforge.benchmarks;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.engine.*;
import dev.tickforge.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
    value = 2,
    jvmArgsAppend = {"-Xms256m", "-Xmx256m"})
public class ReplayBenchmark {
  private static final Set<String> SYMBOLS = Set.of("TEST_A");

  @State(Scope.Thread)
  public static class Core {
    RunConfig config =
        new RunConfig(
            1000000,
            1,
            10,
            100,
            100,
            1000,
            0,
            10000,
            5,
            false,
            false,
            Map.of("TEST_A", new RunConfig.Threshold(100, 120)));
    List<EventEnvelope> events =
        List.of(
            CsvEventReader.parse(1, "1,1,TEST_A,QUOTE,99,20,101,20,,", SYMBOLS),
            CsvEventReader.parse(2, "2,2,TEST_A,TRADE,,,,,100,10", SYMBOLS),
            CsvEventReader.parse(3, "3,3,TEST_A,QUOTE,99,20,101,3,,", SYMBOLS));
    TradingEngine engine;

    @Setup(Level.Invocation)
    public void reset() {
      engine = new TradingEngine(config, new EngineState(config.initialCash()));
    }
  }

  @Benchmark
  public EventEnvelope parser() {
    return CsvEventReader.parse(1, "1,1,TEST_A,QUOTE,99,20,101,20,,", SYMBOLS);
  }

  @Benchmark
  public EngineState quoteSignalAndPartialFill(Core core) {
    for (var e : core.events) core.engine.process(e);
    return core.engine.state();
  }
}
