package com.obsidiandynamics.blackstrom.scheduler;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import com.obsidiandynamics.blackstrom.worker.*;

/**
 *  A scheduler for dispatching arbitrary tasks.
 */
public final class TaskScheduler {
  /** Maximum sleep time. If the next task's time is longer, the sleep will be performed in a loop.
   *  This is also the default time that the scheduler sleeps for if it has no pending tasks. */
  private static final long MAX_SLEEP_NANOS = 1_000_000_000l;
  
  /** Minimum sleep time. Below this threshold sleeping isn't worthwhile. */
  private static final long MIN_SLEEP_NANOS = 1_000_000l;
  
  /** Compensation for the overhead of scheduling a task. */
  private static final long ADJ_NANOS = 500_000l;
  
  /** List of pending tasks, ordered with the most immediate at the head. */
  private final NavigableSet<Task> tasks = new ConcurrentSkipListSet<>(TaskScheduler::compare);
  
  @SuppressWarnings("unchecked")
  private static int compare(Task t1, Task t2) {
    final int timeComp = Long.compare(t1.getTime(), t2.getTime());
    if (timeComp != 0) {
      return timeComp;
    } else {
      @SuppressWarnings("rawtypes")
      final Comparable c1 = t1.getId();
      @SuppressWarnings("rawtypes")
      final Comparable c2 = t2.getId();
      return c1.compareTo(c2);
    }
  }
  
  /** Lock for the scheduler thread to sleep on; can be used to wake the thread. */
  private final Object sleepLock = new Object();
  
  /** The worker thread for performing task execution. */
  private final WorkerThread executor;
  
  /** The time when the thread should be woken, in absolute nanoseconds. See {@link System.nanoTime()}. */
  private volatile long nextWake;
  
  /** Whether execution should be forced for all tasks (regarding of their scheduled time), pending and future. */
  private volatile boolean forceExecute;
  
  /** Atomically assigns numbers for scheduler thread naming. */
  private static final AtomicInteger nextSchedulerThreadNo = new AtomicInteger();
  
  public TaskScheduler() {
    this("TaskScheduler-" + nextSchedulerThreadNo.getAndIncrement());
  }
  
  public TaskScheduler(String threadName) {
    executor = WorkerThread.builder()
        .withOptions(new WorkerOptions()
                     .withName(threadName)
                     .withDaemon(true))
        .withWorker(this::cycle)
        .build();
  }
  
  public void start() {
    executor.start();
  }
  
  public void clear() {
    tasks.clear();
  }
  
  /**
   *  Terminates the scheduler, shutting down the worker thread and preventing further 
   *  task executions.
   */
  public void terminate() {
    executor.terminate();
  }
  
  /**
   *  Waits until the worker thread terminates.<p>
   *  
   *  This method suppresses an {@link InterruptedException} and will re-assert the interrupt 
   *  prior to returning.
   */
  public void joinQuietly() {
    executor.joinQuietly();
  }
  
  /**
   *  Waits until the worker thread terminates.
   *  
   *  @throws InterruptedException If the thread is interrupted.
   */
  public void join() throws InterruptedException {
    executor.join();
  }
  
  private void cycle(WorkerThread thread) {
    synchronized (sleepLock) {
      try {
        delay(tasks.first().getTime());
      } catch (NoSuchElementException e) {
        delay(System.nanoTime() + MAX_SLEEP_NANOS);
      }
    }

    scheduleSingle();
  }
  
  /**
   *  Schedules a single task from the head of the queue, if one is pending and its 
   *  time has come.
   */
  private void scheduleSingle() {
    try {
      final Task first = tasks.first();
      if (forceExecute || System.nanoTime() >= first.getTime() - ADJ_NANOS) {
        if (tasks.remove(first)) {
          first.execute(this);
        }
      }
    } catch (NoSuchElementException e) {} // in case the task was aborted in the meantime
  }
  
  /**
   *  Schedules a task for execution.
   *  
   *  @param task The task to schedule.
   */
  public void schedule(Task task) {
    tasks.add(task);
    if (task.getTime() < nextWake) {
      synchronized (sleepLock) {
        if (task.getTime() < nextWake) {
          nextWake = task.getTime();
          sleepLock.notify();
        }
      }
    }
  }
  
  /**
   *  Removes the given task from the schedule. Once definitively removed, the scheduler will
   *  not execute the task.
   *  
   *  @param task The task to abort.
   *  @return Whether the task was in the schedule (and hence was removed).
   */
  public boolean abort(Task task) {
    return tasks.remove(task);
  }
  
  /**
   *  Puts the scheduler thread to sleep until the given time.<p>
   *  
   *  Requires {@code sleepLock} to be acquired before calling.
   *  
   *  @param until The wake time, in absolute nanoseconds (see {@link System#nanoTime()}).
   */
  private void delay(long until) {
    nextWake = until;
    while (! forceExecute) {
      final long timeDiff = Math.min(MAX_SLEEP_NANOS, nextWake - System.nanoTime() - ADJ_NANOS);
      try {
        if (timeDiff >= MIN_SLEEP_NANOS) {
          final long millis = timeDiff / 1_000_000l;
          final int nanos = (int) (timeDiff - millis * 1_000_000l);
          sleepLock.wait(millis, nanos);
        } else {
          break;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
  
  /**
   *  Forces the immediate execution of all pending tasks, and all future tasks yet to be enqueued.
   */
  public void forceExecute() {
    forceExecute = true;
    synchronized (sleepLock) {
      sleepLock.notify();
    }
  }
  
  /**
   *  Forces the execution of a given task.<p>
   *  
   *  This method is asynchronous, returning as soon as the resulting signal is enqueued.
   *  
   *  @param task The task to time out.
   */
  public void executeNow(Task task) {
    if (abort(task)) {
      task.execute(this);
    }
  }
}