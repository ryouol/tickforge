package dev.tickforge.risk;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.domain.Ledger.Side;
import dev.tickforge.domain.MarketEvent.Quote;

public final class RiskPolicy {
  private RiskPolicy() {}

  public static String reject(
      RunConfig c,
      Side side,
      int quantity,
      int holding,
      long cash,
      Quote quote,
      long time,
      long cap) {
    if (quantity > c.maxOrderQuantity()) return "MAX_ORDER_QUANTITY";
    if (quote == null || time - quote.time() > c.quoteFreshnessNs())
      return "STALE_OR_MISSING_QUOTE";
    if ((side == Side.BUY ? quote.askSize() : quote.bidSize()) == 0) return "NO_LIQUIDITY";
    if (side == Side.SELL) return quantity > holding ? "INSUFFICIENT_POSITION" : null;
    if ((long) holding + quantity > c.maxPosition()) return "POSITION_LIMIT";
    try {
      if (Math.multiplyExact(Math.addExact(cap, c.feePerShare()), quantity) > cash)
        return "INSUFFICIENT_CASH";
    } catch (ArithmeticException e) {
      return "ARITHMETIC_OVERFLOW";
    }
    return null;
  }
}
