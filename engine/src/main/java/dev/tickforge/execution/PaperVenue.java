package dev.tickforge.execution;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.domain.Ledger.*;
import dev.tickforge.domain.MarketEvent.Quote;

public final class PaperVenue {
  private PaperVenue() {}

  public static int quantity(Order o, Quote q, int holding, long cash, RunConfig c) {
    if (o.side() == Side.SELL) return Math.min(o.quantity(), Math.min(q.bidSize(), holding));
    if (q.ask() > o.cap()) return 0;
    long unit = Math.addExact(q.ask(), c.feePerShare());
    return (int)
        Math.min(
            Math.min(o.quantity(), q.askSize()),
            Math.min(Math.max(0, c.maxPosition() - holding), cash / unit));
  }
}
