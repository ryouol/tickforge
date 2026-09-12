package dev.tickforge.io;

import dev.tickforge.domain.MarketEvent;

public record EventEnvelope(long index, Long sequence, MarketEvent event, String error) {}
