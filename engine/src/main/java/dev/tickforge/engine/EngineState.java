package dev.tickforge.engine;

import dev.tickforge.domain.Ledger.*;
import dev.tickforge.domain.MarketEvent.Quote;
import java.util.*;

/** Mutable state belongs exclusively to the engine thread; serialize only at commit boundaries. */
public final class EngineState {
  public int version = 1;
  public long nextIndex = 1,
      lastSequence = 0,
      lastTime = 0,
      cash,
      fees,
      fillCount,
      rejections,
      duplicates,
      quarantined;
  public boolean finalized;
  public Map<String, Quote> quotes = new TreeMap<>();
  public Map<String, Position> positions = new TreeMap<>();
  public Map<String, Order> pending = new TreeMap<>();

  public EngineState() {}

  public EngineState(long cash) {
    this.cash = cash;
  }
}
