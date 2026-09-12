package dev.tickforge.domain;

public final class Ledger {
  private Ledger() {}

  public enum Side {
    BUY,
    SELL
  }

  public enum Status {
    CREATED,
    REJECTED,
    ACCEPTED,
    FILLED,
    PARTIALLY_FILLED,
    CANCELLED
  }

  public record Position(int quantity, long costTicks) {}

  public record Order(
      String id,
      String symbol,
      Side side,
      int quantity,
      int filled,
      Status status,
      long triggerIndex,
      long triggerTime,
      long cap,
      long eligibleTime,
      long expiry,
      int transitions) {
    public Order transition(Status next, int fill) {
      return new Order(
          id,
          symbol,
          side,
          quantity,
          fill,
          next,
          triggerIndex,
          triggerTime,
          cap,
          eligibleTime,
          expiry,
          transitions + 1);
    }
  }

  public record OrderEvent(
      String orderId, int index, Status from, Status to, long causeIndex, String reason) {}

  public record Fill(
      String orderId, int index, long executionIndex, long price, int quantity, long fee) {}

  public record Outcome(
      long physicalIndex, Long sequence, String type, String disposition, String reason) {}
}
