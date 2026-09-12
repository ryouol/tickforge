package dev.tickforge.strategy;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.domain.Ledger.Side;
import dev.tickforge.domain.MarketEvent.Trade;

public final class ThresholdStrategy {
  private ThresholdStrategy() {}

  public static Side signal(Trade trade, int holding, RunConfig.Threshold threshold) {
    if (holding == 0 && trade.price() <= threshold.entry()) return Side.BUY;
    if (holding > 0 && trade.price() >= threshold.exit()) return Side.SELL;
    return null;
  }
}
