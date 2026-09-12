package dev.tickforge.io;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

public record DatasetManifest(
    int formatVersion,
    String generatorVersion,
    long seed,
    long recordCount,
    List<String> symbols,
    int priceScale,
    String sha256,
    String provenance) {
  public static String hash(Path path) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      try {
        MessageDigest d = MessageDigest.getInstance("SHA-256");
        byte[] b = new byte[65536];
        int n;
        while ((n = in.read(b)) != -1) d.update(b, 0, n);
        return HexFormat.of().formatHex(d.digest());
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public static String hashText(String s) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  public static DatasetManifest verify(Path path, Set<String> allowed) throws IOException {
    DatasetManifest m =
        Json.decode(Files.readString(Path.of(path + ".manifest.json")), DatasetManifest.class);
    if (m.formatVersion != 1
        || m.priceScale != 10000
        || m.recordCount < 0
        || m.symbols == null
        || m.symbols.isEmpty()
        || !allowed.containsAll(m.symbols)
        || m.provenance == null
        || m.provenance.isBlank()
        || !hash(path).equals(m.sha256))
      throw new IllegalArgumentException("dataset manifest mismatch");
    return m;
  }

  public static void generate(Path path, long seed, long count) throws IOException {
    if (count < 1 || count > Long.MAX_VALUE / 10000000L)
      throw new IllegalArgumentException("invalid event count");
    Random random = new Random(seed);
    List<String> symbols = List.of("TEST_A", "TEST_B");
    try (BufferedWriter out = Files.newBufferedWriter(path)) {
      out.write(CsvEventReader.HEADER + "\n");
      for (long i = 1; i <= count; i++) {
        String s = symbols.get((int) (((i - 1) / 4) % 2));
        long price = ((i - 1) / 8) % 2 == 0 ? 999000 : 1003000;
        if (i % 2 == 1)
          out.write(
              i
                  + ","
                  + (i * 10000000)
                  + ","
                  + s
                  + ",QUOTE,"
                  + (price - 100)
                  + ","
                  + (1 + random.nextInt(20))
                  + ","
                  + (price + 100)
                  + ","
                  + (1 + random.nextInt(20))
                  + ",,\n");
        else out.write(i + "," + (i * 10000000) + "," + s + ",TRADE,,,,," + price + ",10\n");
      }
    }
    Files.writeString(
        Path.of(path + ".manifest.json"),
        Json.encode(
                new DatasetManifest(
                    1,
                    "tickforge-synthetic-1",
                    seed,
                    count,
                    symbols,
                    10000,
                    hash(path),
                    "Synthetic fictional data; CC0-1.0"))
            + "\n");
  }
}
