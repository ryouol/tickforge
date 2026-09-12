package dev.tickforge.io;

import java.nio.file.*;
import java.util.stream.Stream;

public final class BuildIdentity {
  private BuildIdentity() {}

  public static String current() {
    try {
      Path source =
          Path.of(BuildIdentity.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      if (Files.isRegularFile(source)) return DatasetManifest.hash(source);
      StringBuilder hashes = new StringBuilder();
      try (Stream<Path> files = Files.walk(source)) {
        for (Path p : files.filter(Files::isRegularFile).sorted().toList())
          hashes
              .append(source.relativize(p))
              .append(':')
              .append(DatasetManifest.hash(p))
              .append('\n');
      }
      return DatasetManifest.hashText(hashes.toString());
    } catch (Exception e) {
      throw new IllegalStateException("cannot identify engine build", e);
    }
  }
}
