package dev.tickforge.engine;

import dev.tickforge.domain.Ledger.*;
import java.util.*;

public final class BatchResult {
  public final List<Outcome> outcomes = new ArrayList<>();
  public final List<OrderEvent> transitions = new ArrayList<>();
  public final List<Fill> fills = new ArrayList<>();
  public final Map<String, Order> orders = new LinkedHashMap<>();

  public boolean isEmpty() {
    return outcomes.isEmpty() && transitions.isEmpty();
  }
}
