package dev.tickforge.persistence;

import dev.tickforge.engine.*;

public interface RunRepository extends AutoCloseable {
  void commitBatch(BatchResult batch, EngineState state) throws Exception;

  @Override
  void close() throws Exception;
}
