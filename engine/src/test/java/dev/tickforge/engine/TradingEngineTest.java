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
}
