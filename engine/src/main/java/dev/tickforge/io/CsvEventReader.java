package dev.tickforge.io;

import dev.tickforge.domain.MarketEvent;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.commons.csv.*;

/** One physical line per record; embedded newlines are deliberately outside format v1. */
public final class CsvEventReader implements AutoCloseable {
  public static final String HEADER =
      "sequence,event_time_ns,symbol,type,bid_ticks,bid_size,ask_ticks,ask_size,trade_ticks,trade_size";
  private final BufferedReader reader;
  private final Set<String> symbols;
  private long index;

  public CsvEventReader(Path path, Set<String> symbols) throws IOException {
    reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
    this.symbols = Set.copyOf(symbols);
    try {
      if (!HEADER.equals(line())) throw new IOException("CSV header mismatch");
    } catch (IOException | RuntimeException e) {
      reader.close();
      throw e;
    }
  }

  private String line() throws IOException {
    StringBuilder b = new StringBuilder();
    int c;
    while ((c = reader.read()) != -1 && c != '\n') {
      if (b.length() == 16384) throw new IOException("record exceeds 16384 characters");
      b.append((char) c);
    }
    if (c == -1 && b.isEmpty()) return null;
    if (!b.isEmpty() && b.charAt(b.length() - 1) == '\r') b.setLength(b.length() - 1);
    return b.toString();
  }

  public EventEnvelope next() throws IOException {
    String s = line();
    return s == null ? null : parse(++index, s, symbols);
  }

  public static EventEnvelope parse(long index, String line, Set<String> symbols) {
    Long seq = null;
    try (CSVParser parser = CSVFormat.RFC4180.parse(new StringReader(line))) {
      List<CSVRecord> rows = parser.getRecords();
      if (rows.size() != 1) throw new IllegalArgumentException("expected one record");
      CSVRecord r = rows.getFirst();
      if (r.size() > 0) seq = Long.valueOf(r.get(0));
      if (r.size() != 10) throw new IllegalArgumentException("expected 10 fields");
      long time = Long.parseLong(r.get(1));
      String symbol = r.get(2);
      if (seq == null || seq <= 0 || time < 0 || !symbols.contains(symbol))
        throw new IllegalArgumentException("invalid sequence, timestamp or symbol");
      MarketEvent event;
      if (r.get(3).equals("QUOTE")) {
        long bid = Long.parseLong(r.get(4)), ask = Long.parseLong(r.get(6));
        int bs = Integer.parseInt(r.get(5)), as = Integer.parseInt(r.get(7));
        if (bid <= 0 || ask < bid || bs < 0 || as < 0 || !r.get(8).isEmpty() || !r.get(9).isEmpty())
          throw new IllegalArgumentException("invalid quote");
        event = new MarketEvent.Quote(seq, time, symbol, bid, bs, ask, as);
      } else if (r.get(3).equals("TRADE")) {
        long price = Long.parseLong(r.get(8));
        int size = Integer.parseInt(r.get(9));
        if (price <= 0 || size <= 0) throw new IllegalArgumentException("invalid trade");
        for (int i = 4; i < 8; i++)
          if (!r.get(i).isEmpty()) throw new IllegalArgumentException("trade has quote fields");
        event = new MarketEvent.Trade(seq, time, symbol, price, size);
      } else throw new IllegalArgumentException("unknown event type");
      return new EventEnvelope(index, seq, event, null);
    } catch (IOException | UncheckedIOException | IllegalArgumentException e) {
      return new EventEnvelope(index, seq, null, "MALFORMED: " + e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
