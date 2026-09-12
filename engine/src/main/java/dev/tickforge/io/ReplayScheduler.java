package dev.tickforge.io;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class ReplayScheduler {
  private final double speed;
  private long firstTime = -1, lastTime, start;

  public ReplayScheduler(double speed) {
    if (!Double.isFinite(speed) || speed < 0)
      throw new IllegalArgumentException("speed must be finite and nonnegative");
    this.speed = speed;
  }

  public long await(long eventTime, AtomicBoolean stop) throws InterruptedException {
    if (speed == 0) return System.nanoTime();
    if (firstTime < 0) {
      firstTime = eventTime;
      start = System.nanoTime();
      lastTime = eventTime;
    }
    lastTime = Math.max(lastTime, eventTime);
    double delta = (lastTime - firstTime) / speed;
    if (delta > Long.MAX_VALUE / 2)
      throw new IllegalArgumentException("paced duration exceeds monotonic timing range");
    long offset = (long) delta;
    while (!stop.get()) {
      long remaining = offset - (System.nanoTime() - start);
      if (remaining <= 0) break;
      LockSupport.parkNanos(Math.min(remaining, 10000000));
      if (Thread.interrupted()) throw new InterruptedException();
    }
    return start + offset;
  }
}
