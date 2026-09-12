package dev.tickforge.engine;

import static org.junit.jupiter.api.Assertions.*;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.domain.Ledger.*;
import dev.tickforge.io.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class TradingEngineTest {
  static RunConfig config() {
    return new RunConfig(
        100000,
        2,
        10,
        20,
        20,
        100,
        0,
        1000,
        5,
        false,
        false,
        Map.of("TEST_A", new RunConfig.Threshold(100, 120)));
  }

  TradingEngine engine = new TradingEngine(config(), new EngineState(100000));
  long index;

  void row(String row) {
    engine.process(CsvEventReader.parse(++index, row, Set.of("TEST_A")));
  }

  @Test
  void noLookaheadPartialIocAndExactCash() {
    row("1,1,TEST_A,QUOTE,99,20,101,20,,");
    row("2,2,TEST_A,TRADE,,,,,100,10");
    assertEquals(0, engine.state().fillCount);
    assertEquals(1, engine.state().pending.size());
    row("3,2,TEST_A,QUOTE,99,20,101,3,,");
    assertEquals(99691, engine.state().cash);
    assertEquals(6, engine.state().fees);
    assertEquals(3, engine.state().positions.get("TEST_A").quantity());
    assertTrue(engine.state().pending.isEmpty());
    assertEquals(
        List.of(Status.ACCEPTED, Status.PARTIALLY_FILLED, Status.CANCELLED),
        engine.batch().transitions.stream().map(OrderEvent::to).toList());
    row("4,3,TEST_A,TRADE,,,,,120,10");
    row("5,4,TEST_A,QUOTE,119,20,121,20,,");
    assertEquals(100042, engine.state().cash);
    assertEquals(12, engine.state().fees);
    assertEquals(0, engine.state().positions.get("TEST_A").quantity());
  }

  @Test
  void duplicateAndOldTimestampNeverOverwrite() {
    row("1,10,TEST_A,QUOTE,99,10,101,10,,");
    row("1,11,TEST_A,QUOTE,1,10,2,10,,");
    row("2,9,TEST_A,QUOTE,3,10,4,10,,");
    assertEquals(99, engine.state().quotes.get("TEST_A").bid());
    assertEquals(2, engine.state().lastSequence);
    assertEquals(1, engine.state().duplicates);
    assertEquals(1, engine.state().quarantined);
  }

  @Test
  void gapAndMalformedHaltBeforeCheckpoint() {
    assertThrows(IllegalArgumentException.class, () -> row("2,1,TEST_A,TRADE,,,,,100,10"));
    assertEquals(1, engine.state().nextIndex);
  }

  @Test
  void capCancelsAndEofIsIdempotent() {
    row("1,1,TEST_A,QUOTE,99,20,101,20,,");
    row("2,2,TEST_A,TRADE,,,,,100,10");
    row("3,3,TEST_A,QUOTE,109,20,111,20,,");
    assertEquals(0, engine.state().fillCount);
    row("4,4,TEST_A,TRADE,,,,,100,10");
    engine.finish();
    int n = engine.batch().transitions.size();
    engine.finish();
    assertEquals(n, engine.batch().transitions.size());
    assertTrue(engine.state().pending.isEmpty());
  }

  @Test
  void staleAndOverflowRiskRejected() {
    row("1,1,TEST_A,QUOTE,99,20,101,20,,");
    row("2,200,TEST_A,TRADE,,,,,100,10");
    assertEquals("STALE_OR_MISSING_QUOTE", engine.batch().transitions.getFirst().reason());
    row("3,201,TEST_A,QUOTE,1,20,9223372036854775807,20,,");
    row("4,202,TEST_A,TRADE,,,,,100,10");
    assertEquals("ARITHMETIC_OVERFLOW", engine.batch().transitions.getLast().reason());
  }

  @Test
  void timeoutBeforeQuoteAndZeroSizeCannotFill() {
    row("1,1,TEST_A,QUOTE,99,20,101,20,,");
    row("2,2,TEST_A,TRADE,,,,,100,10");
    row("3,1002,TEST_A,QUOTE,99,20,101,20,,");
    assertEquals("TIMEOUT", engine.batch().transitions.getLast().reason());
    assertEquals(0, engine.state().fillCount);
    row("4,1003,TEST_A,TRADE,,,,,100,10");
    row("5,1004,TEST_A,QUOTE,99,20,101,0,,");
    assertEquals("IOC_NO_FILL", engine.batch().transitions.getLast().reason());
    assertEquals(100000, engine.state().cash);
  }

  @Test
  void quarantineStillEnforcesGapAndAllowsExplicitDiagnosticMode() {
    var c = config();
    var diagnostic =
        new RunConfig(
            c.initialCash(),
            c.feePerShare(),
            c.orderQuantity(),
            c.maxOrderQuantity(),
            c.maxPosition(),
            c.quoteFreshnessNs(),
            c.latencyNs(),
            c.timeoutNs(),
            c.buySlippageTicks(),
            true,
            false,
            c.symbols());
    var e = new TradingEngine(diagnostic, new EngineState(c.initialCash()));
    e.process(CsvEventReader.parse(1, "garbage", c.symbols().keySet()));
    assertEquals(2, e.state().nextIndex);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            e.process(
                CsvEventReader.parse(2, "2,1,TEST_A,TRADE,,,,,100,10", c.symbols().keySet())));
    assertEquals(2, e.state().nextIndex);
    var gaps =
        new RunConfig(
            c.initialCash(),
            c.feePerShare(),
            c.orderQuantity(),
            c.maxOrderQuantity(),
            c.maxPosition(),
            c.quoteFreshnessNs(),
            c.latencyNs(),
            c.timeoutNs(),
            c.buySlippageTicks(),
            true,
            true,
            c.symbols());
    var tolerant = new TradingEngine(gaps, new EngineState(c.initialCash()));
    tolerant.process(
        CsvEventReader.parse(1, "5,1,TEST_A,QUOTE,99,1,101,1,,", c.symbols().keySet()));
    assertEquals("TEST_ONLY_GAP_TOLERATED", tolerant.batch().outcomes.getFirst().reason());
  }

  @Test
  void cashAndPositionAreRecheckedAcrossOutstandingSymbols() {
    var c =
        new RunConfig(
            1500,
            2,
            10,
            10,
            10,
            1000,
            0,
            10000,
            0,
            false,
            false,
            Map.of(
                "TEST_A",
                new RunConfig.Threshold(100, 120),
                "TEST_B",
                new RunConfig.Threshold(100, 120)));
    var e = new TradingEngine(c, new EngineState(c.initialCash()));
    var lines =
        List.of(
            "1,1,TEST_A,QUOTE,99,20,100,20,,",
            "2,2,TEST_B,QUOTE,99,20,100,20,,",
            "3,3,TEST_A,TRADE,,,,,100,10",
            "4,4,TEST_B,TRADE,,,,,100,10",
            "5,5,TEST_A,QUOTE,99,20,100,20,,",
            "6,6,TEST_B,QUOTE,99,20,100,20,,");
    for (int i = 0; i < lines.size(); i++)
      e.process(CsvEventReader.parse(i + 1, lines.get(i), c.symbols().keySet()));
    assertEquals(72, e.state().cash);
    assertEquals(10, e.state().positions.get("TEST_A").quantity());
    assertEquals(4, e.state().positions.get("TEST_B").quantity());
    assertTrue(e.state().pending.isEmpty());
  }

  @Test
  void nonzeroLatencyWaitsUntilEligibility() {
    var c = config();
    var delayed =
        new RunConfig(
            c.initialCash(),
            c.feePerShare(),
            c.orderQuantity(),
            c.maxOrderQuantity(),
            c.maxPosition(),
            c.quoteFreshnessNs(),
            10,
            c.timeoutNs(),
            c.buySlippageTicks(),
            false,
            false,
            c.symbols());
    var e = new TradingEngine(delayed, new EngineState(c.initialCash()));
    var rows =
        List.of(
            "1,1,TEST_A,QUOTE,99,20,101,20,,",
            "2,2,TEST_A,TRADE,,,,,100,10",
            "3,11,TEST_A,QUOTE,99,20,101,20,,");
    for (int i = 0; i < rows.size(); i++)
      e.process(CsvEventReader.parse(i + 1, rows.get(i), c.symbols().keySet()));
    assertEquals(0, e.state().fillCount);
    assertEquals(1, e.state().pending.size());
    e.process(CsvEventReader.parse(4, "4,12,TEST_A,QUOTE,99,20,101,20,,", c.symbols().keySet()));
    assertEquals(1, e.state().fillCount);
    assertTrue(e.state().pending.isEmpty());
  }

  @Test
  void strictMalformedInputDoesNotAdvanceCheckpoint() {
    row("1,1,TEST_A,QUOTE,99,20,101,20,,");
    assertThrows(IllegalArgumentException.class, () -> row("malformed"));
    assertEquals(2, engine.state().nextIndex);
    assertEquals(1, engine.state().lastSequence);
    assertEquals(1, engine.batch().outcomes.size());
  }

  @Test
  void acceptanceLimitsRejectWithoutFinancialChanges() {
    assertRejected(
        new RunConfig(100000, 2, 10, 9, 20, 100, 0, 1000, 5, false, false, config().symbols()),
        "MAX_ORDER_QUANTITY");
    assertRejected(
        new RunConfig(100000, 2, 10, 20, 9, 100, 0, 1000, 5, false, false, config().symbols()),
        "POSITION_LIMIT");
    assertRejected(
        new RunConfig(1079, 2, 10, 20, 20, 100, 0, 1000, 5, false, false, config().symbols()),
        "INSUFFICIENT_CASH");
    var c = new RunConfig(1080, 2, 10, 20, 20, 100, 0, 1000, 5, false, false, config().symbols());
    var e = new TradingEngine(c, new EngineState(c.initialCash()));
    e.process(CsvEventReader.parse(1, "1,1,TEST_A,QUOTE,99,20,101,20,,", c.symbols().keySet()));
    e.process(CsvEventReader.parse(2, "2,2,TEST_A,TRADE,,,,,100,10", c.symbols().keySet()));
    assertEquals(1, e.state().pending.size());
  }

  private void assertRejected(RunConfig c, String reason) {
    var e = new TradingEngine(c, new EngineState(c.initialCash()));
    e.process(CsvEventReader.parse(1, "1,1,TEST_A,QUOTE,99,20,101,20,,", c.symbols().keySet()));
    e.process(CsvEventReader.parse(2, "2,2,TEST_A,TRADE,,,,,100,10", c.symbols().keySet()));
    assertEquals(reason, e.batch().transitions.getLast().reason());
    assertTrue(e.state().pending.isEmpty());
    assertEquals(c.initialCash(), e.state().cash);
    assertEquals(0, e.state().fees);
    assertTrue(e.state().positions.isEmpty());
  }
}
