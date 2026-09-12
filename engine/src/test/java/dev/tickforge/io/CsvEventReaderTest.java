package dev.tickforge.io;

import static org.junit.jupiter.api.Assertions.*;

import dev.tickforge.domain.MarketEvent;
import java.nio.file.*;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvEventReaderTest {
  @TempDir Path dir;

  @Test
  void exactAndInvalid() {
    var e = CsvEventReader.parse(1, "1,100,TEST_A,QUOTE,1000000,0,1000200,10,,", Set.of("TEST_A"));
    assertEquals(1000200, ((MarketEvent.Quote) e.event()).ask());
    for (String s :
        new String[] {
          "1,100,TEST_A,QUOTE,3,1,2,1,,",
          "1,100,TEST_A,TRADE,,,,,9223372036854775808,1",
          "1,100,TEST_A,QUOTE,1,-1,2,1,,",
          "1,100,OTHER,TRADE,,,,,1,1",
          "1,100,TEST_A,TRADE,1,,,,1,1",
          "\"unterminated"
        }) assertNotNull(CsvEventReader.parse(1, s, Set.of("TEST_A")).error());
  }

  @Test
  void generatedDataHasVerifiedManifest() throws Exception {
    Path p = dir.resolve("events.csv");
    DatasetManifest.generate(p, 42, 10);
    assertEquals(10, DatasetManifest.verify(p, Set.of("TEST_A", "TEST_B")).recordCount());
    try (var r = new CsvEventReader(p, Set.of("TEST_A", "TEST_B"))) {
      for (int i = 1; i <= 10; i++) {
        var e = r.next();
        assertEquals(i, e.index());
        assertNull(e.error());
      }
      assertNull(r.next());
    }
    Files.writeString(p, "tampered", StandardOpenOption.APPEND);
    assertThrows(
        IllegalArgumentException.class,
        () -> DatasetManifest.verify(p, Set.of("TEST_A", "TEST_B")));
  }
}
