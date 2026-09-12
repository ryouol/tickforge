package dev.tickforge.domain;

public sealed interface MarketEvent permits MarketEvent.Quote, MarketEvent.Trade {
  long sequence();

  long time();

  String symbol();

  record Quote(
      long sequence, long time, String symbol, long bid, int bidSize, long ask, int askSize)
      implements MarketEvent {}

  record Trade(long sequence, long time, String symbol, long price, int quantity)
      implements MarketEvent {}
}
