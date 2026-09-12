package dev.tickforge.engine;

import dev.tickforge.cli.RunConfig;
import dev.tickforge.domain.*;
import dev.tickforge.domain.Ledger.*;
import dev.tickforge.domain.MarketEvent.*;
import dev.tickforge.execution.PaperVenue;
import dev.tickforge.io.EventEnvelope;
import dev.tickforge.risk.RiskPolicy;
import dev.tickforge.strategy.ThresholdStrategy;
import java.util.*;

public final class TradingEngine {
  private final RunConfig config;
  private final EngineState state;
  private BatchResult batch = new BatchResult();

  public TradingEngine(RunConfig config, EngineState state) {
    this.config = config;
    this.state = state;
    if (state.version != 1) throw new IllegalArgumentException("unsupported snapshot version");
  }

  public EngineState state() {
    return state;
  }

  public BatchResult batch() {
    return batch;
  }

  public void committed() {
    batch = new BatchResult();
  }

  public void process(EventEnvelope e) {
    if (state.finalized || e.index() != state.nextIndex)
      throw new IllegalStateException("invalid checkpoint input");
    if (e.error() != null) {
      if (!config.quarantineMalformed())
        throw new IllegalArgumentException("row " + e.index() + ": " + e.error());
      state.quarantined++;
      outcome(e, "QUARANTINED", e.error());
      return;
    }
    MarketEvent event = e.event();
    if (event.sequence() <= state.lastSequence) {
      state.duplicates++;
      outcome(e, "IGNORED", "DUPLICATE_OR_OLDER_SEQUENCE");
      return;
    }
    boolean gap = event.sequence() - state.lastSequence != 1;
    if (gap && !config.tolerateGaps())
      throw new IllegalArgumentException("sequence gap at row " + e.index());
    state.lastSequence = event.sequence();
    if (event.time() < state.lastTime) {
      state.quarantined++;
      outcome(e, "QUARANTINED", "OLDER_TIMESTAMP");
      return;
    }
    state.lastTime = event.time();
    for (Order order : List.copyOf(state.pending.values()))
      if (event.time() >= order.expiry()) cancel(order, e.index(), "TIMEOUT");
    switch (event) {
      case Quote q -> {
        state.quotes.put(q.symbol(), q);
        Order o = state.pending.get(q.symbol());
        if (o != null && e.index() > o.triggerIndex() && q.time() >= o.eligibleTime())
          execute(o, q, e.index());
      }
      case Trade t -> {
        if (!state.pending.containsKey(t.symbol())) {
          int holding = position(t.symbol()).quantity();
          Side side = ThresholdStrategy.signal(t, holding, config.symbols().get(t.symbol()));
          if (side != null)
            create(
                t,
                e.index(),
                side,
                side == Side.BUY
                    ? config.orderQuantity()
                    : Math.min(holding, config.orderQuantity()));
        }
      }
    }
    outcome(e, "ACCEPTED", gap ? "TEST_ONLY_GAP_TOLERATED" : "OK");
  }

  private void outcome(EventEnvelope e, String disposition, String reason) {
    batch.outcomes.add(
        new Outcome(
            e.index(),
            e.sequence(),
            e.event() == null ? "INVALID" : e.event() instanceof Quote ? "QUOTE" : "TRADE",
            disposition,
            reason));
    state.nextIndex = Math.incrementExact(state.nextIndex);
  }

  private Position position(String s) {
    return state.positions.getOrDefault(s, new Position(0, 0));
  }

  private void create(Trade t, long index, Side side, int quantity) {
    Quote q = state.quotes.get(t.symbol());
    long cap = 0, eligible = 0, expiry = 0;
    String reason = null;
    try {
      cap = q == null ? 0 : Math.addExact(q.ask(), config.buySlippageTicks());
      eligible = Math.addExact(t.time(), config.latencyNs());
      expiry = Math.addExact(t.time(), config.timeoutNs());
    } catch (ArithmeticException e) {
      reason = "ARITHMETIC_OVERFLOW";
    }
    Order order =
        new Order(
            index + ":threshold:0",
            t.symbol(),
            side,
            quantity,
            0,
            Status.CREATED,
            index,
            t.time(),
            cap,
            eligible,
            expiry,
            0);
    if (reason == null)
      reason =
          RiskPolicy.reject(
              config,
              side,
              quantity,
              position(t.symbol()).quantity(),
              state.cash,
              q,
              t.time(),
              cap);
    if (reason != null) {
      state.rejections++;
      transition(order, Status.REJECTED, 0, index, reason);
    } else {
      order = transition(order, Status.ACCEPTED, 0, index, "RISK_ACCEPTED");
      state.pending.put(t.symbol(), order);
    }
  }

  private Order transition(Order order, Status next, int filled, long cause, String reason) {
    batch.transitions.add(
        new OrderEvent(order.id(), order.transitions(), order.status(), next, cause, reason));
    Order result = order.transition(next, filled);
    batch.orders.put(result.id(), result);
    return result;
  }

  private void cancel(Order order, long cause, String reason) {
    transition(order, Status.CANCELLED, order.filled(), cause, reason);
    state.pending.remove(order.symbol());
  }

  private void execute(Order order, Quote quote, long index) {
    Position p = position(order.symbol());
    int qty;
    long price = order.side() == Side.BUY ? quote.ask() : quote.bid();
    long fee, cash, cost, fees;
    int holding;
    try {
      qty = PaperVenue.quantity(order, quote, p.quantity(), state.cash, config);
      if (qty == 0) {
        cancel(order, index, "IOC_NO_FILL");
        return;
      }
      fee = Math.multiplyExact(config.feePerShare(), qty);
      long gross = Math.multiplyExact(price, qty);
      fees = Math.addExact(state.fees, fee);
      if (order.side() == Side.BUY) {
        cash = Math.subtractExact(state.cash, Math.addExact(gross, fee));
        holding = Math.addExact(p.quantity(), qty);
        cost = Math.addExact(p.costTicks(), Math.addExact(gross, fee));
      } else {
        cash = Math.addExact(state.cash, Math.subtractExact(gross, fee));
        holding = p.quantity() - qty;
        cost =
            holding == 0
                ? 0
                : Math.subtractExact(
                    p.costTicks(), Math.multiplyExact(p.costTicks(), qty) / p.quantity());
      }
      if (cash < 0) {
        cancel(order, index, "INSUFFICIENT_CASH_FOR_FEES");
        return;
      }
    } catch (ArithmeticException e) {
      cancel(order, index, "ARITHMETIC_OVERFLOW");
      return;
    }
    state.cash = cash;
    state.fees = fees;
    state.fillCount++;
    state.positions.put(order.symbol(), new Position(holding, cost));
    batch.fills.add(new Fill(order.id(), 0, index, price, qty, fee));
    Order filled =
        transition(
            order,
            qty == order.quantity() ? Status.FILLED : Status.PARTIALLY_FILLED,
            qty,
            index,
            "QUOTE_FILL");
    if (qty < order.quantity()) cancel(filled, index, "IOC_REMAINDER");
    else state.pending.remove(order.symbol());
  }

  public void finish() {
    if (state.finalized) return;
    for (Order o : List.copyOf(state.pending.values())) cancel(o, state.nextIndex, "END_OF_INPUT");
    state.finalized = true;
  }
}
