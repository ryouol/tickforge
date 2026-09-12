package dev.tickforge.engine;

import dev.tickforge.io.*;
import dev.tickforge.ops.Metrics;
import dev.tickforge.persistence.RunRepository;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class ReplayRunner {
  public record Options(int batchSize, int queueCapacity, double speed) {
    public Options {
      if (batchSize < 1
          || batchSize > 100000
          || queueCapacity < 1
          || queueCapacity > 1000000
          || !Double.isFinite(speed)
          || speed < 0) throw new IllegalArgumentException("invalid replay options");
    }
  }

  private record Message(
      EventEnvelope event, Exception failure, boolean eof, long admitted, long scheduled) {}

  private final Metrics metrics;
  private final AtomicBoolean stop;

  public ReplayRunner(Metrics metrics, AtomicBoolean stop) {
    this.metrics = metrics;
    this.stop = stop;
  }

  public void run(
      Path input,
      DatasetManifest manifest,
      Set<String> symbols,
      TradingEngine engine,
      RunRepository repository,
      Options options,
      Consumer<String> publish)
      throws Exception {
    if (engine.state().finalized) {
      publish.accept(Json.encode(engine.state()));
      return;
    }
    ArrayBlockingQueue<Message> queue = new ArrayBlockingQueue<>(options.queueCapacity());
    long next = engine.state().nextIndex;
    Thread producer =
        Thread.ofPlatform()
            .name("tickforge-reader")
            .start(() -> produce(input, manifest, symbols, next, options, queue));
    List<Message> batch = new ArrayList<>(options.batchSize());
    try {
      while (!stop.get()) {
        Message message = queue.poll(100, TimeUnit.MILLISECONDS);
        metrics.queueDepth.set(queue.size());
        if (message == null) continue;
        if (message.failure != null) throw message.failure;
        if (message.eof) {
          engine.finish();
          commit(engine, repository, publish, batch);
          return;
        }
        long start = System.nanoTime();
        metrics.record("queue_wait", start - message.admitted);
        engine.process(message.event);
        metrics.record("engine", System.nanoTime() - start);
        batch.add(message);
        if (batch.size() == options.batchSize()) commit(engine, repository, publish, batch);
      }
      if (!batch.isEmpty()) commit(engine, repository, publish, batch);
    } finally {
      stop.set(true);
      producer.interrupt();
      producer.join(5000);
      if (producer.isAlive()) throw new IllegalStateException("producer did not stop");
    }
  }

  private void produce(
      Path input,
      DatasetManifest manifest,
      Set<String> symbols,
      long next,
      Options options,
      ArrayBlockingQueue<Message> queue) {
    try (var reader = new CsvEventReader(input, symbols)) {
      ReplayScheduler scheduler = new ReplayScheduler(options.speed());
      EventEnvelope e;
      long count = 0;
      while (!stop.get() && (e = reader.next()) != null) {
        count = e.index();
        if (count < next) continue;
        metrics.parsed.incrementAndGet();
        long scheduled = scheduler.await(e.event() == null ? 0 : e.event().time(), stop);
        long admitted = System.nanoTime();
        metrics.record("schedule_lateness", admitted - scheduled);
        offer(queue, new Message(e, null, false, admitted, scheduled));
      }
      if (!stop.get()) {
        if (count != manifest.recordCount())
          throw new IllegalArgumentException("manifest record count mismatch");
        offer(queue, new Message(null, null, true, 0, 0));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      try {
        offer(queue, new Message(null, e, false, 0, 0));
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void offer(ArrayBlockingQueue<Message> queue, Message message)
      throws InterruptedException {
    if (queue.offer(message)) {
      metrics.queueDepth.set(queue.size());
      return;
    }
    long start = System.nanoTime();
    try {
      while (!stop.get()) if (queue.offer(message, 100, TimeUnit.MILLISECONDS)) return;
    } finally {
      metrics.blockedNs.addAndGet(System.nanoTime() - start);
      metrics.queueDepth.set(queue.size());
    }
  }

  private void commit(
      TradingEngine engine, RunRepository repository, Consumer<String> publish, List<Message> batch)
      throws Exception {
    long start = System.nanoTime();
    try {
      repository.commitBatch(engine.batch(), engine.state());
    } catch (Exception e) {
      metrics.commitFailures.incrementAndGet();
      throw e;
    }
    long now = System.nanoTime();
    metrics.record("commit", now - start);
    for (Message message : batch) {
      metrics.record("admission_to_commit", now - message.admitted);
      metrics.record("scheduled_to_commit", now - message.scheduled);
    }
    metrics.committed.addAndGet(batch.size());
    publish.accept(Json.encode(engine.state()));
    engine.committed();
    batch.clear();
  }
}
