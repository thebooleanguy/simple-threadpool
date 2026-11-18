package dev.thebooleanguy.threadpool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class AppTest {
    private static final long WAIT_SECONDS = 3;

    @Test
    void multipleWorkersExecuteTasksConcurrently() throws Exception {
        ThreadPool pool = new ThreadPool(2, 2);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);

        try {
            for (int i = 0; i < 2; i++) {
                assertTrue(pool.submit(() -> {
                    started.countDown();
                    await(release);
                }));
            }

            assertTrue(started.await(WAIT_SECONDS, TimeUnit.SECONDS),
                    "both worker tasks should start before either is released");
        } finally {
            release.countDown();
            shutdownAndAwait(pool);
        }
    }

    @Test
    void everyAcceptedTaskExecutesExactlyOnce() throws Exception {
        int taskCount = 100;
        ThreadPool pool = new ThreadPool(taskCount, 4);
        AtomicIntegerArray executions = new AtomicIntegerArray(taskCount);
        CountDownLatch completed = new CountDownLatch(taskCount);

        try {
            for (int i = 0; i < taskCount; i++) {
                int taskIndex = i;
                assertTrue(pool.submit(() -> {
                    executions.incrementAndGet(taskIndex);
                    completed.countDown();
                }));
            }

            assertTrue(completed.await(WAIT_SECONDS, TimeUnit.SECONDS));
            for (int i = 0; i < taskCount; i++) {
                assertEquals(1, executions.get(i), "task " + i + " execution count");
            }
        } finally {
            shutdownAndAwait(pool);
        }
    }

    @Test
    void runtimeExceptionDoesNotKillWorker() throws Exception {
        ThreadPool pool = new ThreadPool(2, 1);
        CountDownLatch followingTaskRan = new CountDownLatch(1);

        try {
            assertTrue(pool.submit(() -> {
                throw new RuntimeException("expected test failure");
            }));
            assertTrue(pool.submit(followingTaskRan::countDown));

            assertTrue(followingTaskRan.await(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            shutdownAndAwait(pool);
        }
    }

    @Test
    void rejectsTasksWhenBoundedQueueIsFull() throws Exception {
        ThreadPool pool = new ThreadPool(1, 1);
        CountDownLatch runningTaskStarted = new CountDownLatch(1);
        CountDownLatch releaseRunningTask = new CountDownLatch(1);
        CountDownLatch queuedTaskRan = new CountDownLatch(1);

        try {
            assertTrue(pool.submit(() -> {
                runningTaskStarted.countDown();
                await(releaseRunningTask);
            }));
            assertTrue(runningTaskStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertTrue(pool.submit(queuedTaskRan::countDown));
            assertFalse(pool.submit(() -> {
            }));

            releaseRunningTask.countDown();
            assertTrue(queuedTaskRan.await(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            releaseRunningTask.countDown();
            shutdownAndAwait(pool);
        }
    }

    @Test
    void rejectsTasksAfterShutdown() throws Exception {
        ThreadPool pool = new ThreadPool(1, 1);
        try {
            pool.shutdown();
            assertFalse(pool.submit(() -> {
            }));
            assertTrue(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            shutdownAndAwait(pool);
        }
    }

    @Test
    void gracefulShutdownFinishesPreviouslyAcceptedTasks() throws Exception {
        ThreadPool pool = new ThreadPool(1, 1);
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch taskCompleted = new CountDownLatch(1);

        try {
            assertTrue(pool.submit(() -> {
                taskStarted.countDown();
                await(releaseTask);
                taskCompleted.countDown();
            }));
            assertTrue(taskStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));

            pool.shutdown();
            assertFalse(pool.submit(() -> {
            }));
            releaseTask.countDown();

            assertTrue(taskCompleted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertTrue(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            releaseTask.countDown();
            shutdownAndAwait(pool);
        }
    }

    @Test
    void awaitTerminationReturnsFalseWhileTaskIsStillRunning() throws Exception {
        ThreadPool pool = new ThreadPool(1, 1);
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);

        try {
            assertTrue(pool.submit(() -> {
                taskStarted.countDown();
                await(releaseTask);
            }));
            assertTrue(taskStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            pool.shutdown();

            assertFalse(pool.awaitTermination(50, TimeUnit.MILLISECONDS));
            releaseTask.countDown();
            assertTrue(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            releaseTask.countDown();
            shutdownAndAwait(pool);
        }
    }

    @Test
    void awaitTerminationReturnsTrueAfterWorkersFinish() throws Exception {
        ThreadPool pool = new ThreadPool(1, 1);
        CountDownLatch taskCompleted = new CountDownLatch(1);

        try {
            assertTrue(pool.submit(taskCompleted::countDown));
            pool.shutdown();

            assertTrue(taskCompleted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertTrue(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            shutdownAndAwait(pool);
        }
    }

    @Test
    void awaitTerminationReturnsTrueImmediatelyWhenAlreadyTerminated() throws Exception {
        ThreadPool pool = new ThreadPool(1, 1);
        try {
            pool.shutdown();
            assertTrue(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
            assertTrue(pool.awaitTermination(0, TimeUnit.SECONDS));
        } finally {
            shutdownAndAwait(pool);
        }
    }

    @Test
    void awaitTerminationReturnsFalseWhilePoolIsRunning() throws Exception {
        ThreadPool pool = new ThreadPool(1, 1);
        try {
            assertFalse(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
        } finally {
            shutdownAndAwait(pool);
        }
    }

    @Test
    void submissionsRacingWithShutdownAreNotAcceptedAfterShutdown() throws Exception {
        int submitterCount = 16;
        ThreadPool pool = new ThreadPool(submitterCount, 2);
        CountDownLatch ready = new CountDownLatch(submitterCount + 1);
        CountDownLatch start = new CountDownLatch(1);
        boolean[] accepted = new boolean[submitterCount];
        AtomicIntegerArray executions = new AtomicIntegerArray(submitterCount);
        Thread[] submitters = new Thread[submitterCount];
        Thread shutdownThread = new Thread(() -> {
            ready.countDown();
            await(start);
            pool.shutdown();
        });

        try {
            for (int i = 0; i < submitterCount; i++) {
                int taskIndex = i;
                submitters[i] = new Thread(() -> {
                    ready.countDown();
                    await(start);
                    accepted[taskIndex] = pool.submit(() -> executions.incrementAndGet(taskIndex));
                });
                submitters[i].start();
            }
            shutdownThread.start();

            assertTrue(ready.await(WAIT_SECONDS, TimeUnit.SECONDS));
            start.countDown();
            joinWithinTimeout(shutdownThread);
            for (Thread submitter : submitters) {
                joinWithinTimeout(submitter);
            }

            assertFalse(pool.submit(() -> {
            }), "submissions after shutdown must be rejected");
            assertTrue(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS));
            for (int i = 0; i < submitterCount; i++) {
                assertEquals(accepted[i] ? 1 : 0, executions.get(i),
                        "task " + i + " should execute once iff accepted");
            }
        } finally {
            start.countDown();
            shutdownAndAwait(pool);
        }
    }

    @Test
    void concurrentShutdownAndAwaitTerminationCallsCompleteCorrectly() throws Exception {
        int callerCount = 6;
        ThreadPool pool = new ThreadPool(1, 1);
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch callersReady = new CountDownLatch(callerCount);
        CountDownLatch startCallers = new CountDownLatch(1);
        CountDownLatch shutdownCallsComplete = new CountDownLatch(callerCount);
        boolean[] results = new boolean[callerCount];
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread[] callers = new Thread[callerCount];

        try {
            assertTrue(pool.submit(() -> {
                taskStarted.countDown();
                await(releaseTask);
            }));
            assertTrue(taskStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));

            for (int i = 0; i < callerCount; i++) {
                int callerIndex = i;
                callers[i] = new Thread(() -> {
                    callersReady.countDown();
                    await(startCallers);
                    try {
                        pool.shutdown();
                        shutdownCallsComplete.countDown();
                        results[callerIndex] = pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS);
                    } catch (Throwable thrown) {
                        failure.compareAndSet(null, thrown);
                    }
                });
                callers[i].start();
            }

            assertTrue(callersReady.await(WAIT_SECONDS, TimeUnit.SECONDS));
            startCallers.countDown();
            assertTrue(shutdownCallsComplete.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertThreadsWaiting(callers);
            releaseTask.countDown();

            for (Thread caller : callers) {
                joinWithinTimeout(caller);
            }
            assertNull(failure.get());
            for (boolean result : results) {
                assertTrue(result);
            }
            assertTrue(pool.awaitTermination(0, TimeUnit.SECONDS));
        } finally {
            startCallers.countDown();
            releaseTask.countDown();
            shutdownAndAwait(pool);
        }
    }

    @Test
    void interruptedAwaitTerminationPropagatesInterruptedException() throws Exception {
        ThreadPool pool = new ThreadPool(1, 1);
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch awaitStarted = new CountDownLatch(1);
        AtomicReference<Throwable> result = new AtomicReference<>();
        Thread awaiter = new Thread(() -> {
            awaitStarted.countDown();
            try {
                pool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (Throwable thrown) {
                result.set(thrown);
            }
        });

        try {
            assertTrue(pool.submit(() -> {
                taskStarted.countDown();
                await(releaseTask);
            }));
            assertTrue(taskStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            pool.shutdown();
            awaiter.start();
            assertTrue(awaitStarted.await(WAIT_SECONDS, TimeUnit.SECONDS));
            assertThreadWaiting(awaiter);
            awaiter.interrupt();
            joinWithinTimeout(awaiter);

            assertInstanceOf(InterruptedException.class, result.get());
        } finally {
            releaseTask.countDown();
            shutdownAndAwait(pool);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void shutdownAndAwait(ThreadPool pool) throws InterruptedException {
        pool.shutdown();
        assertTrue(pool.awaitTermination(WAIT_SECONDS, TimeUnit.SECONDS),
                "pool should terminate during test cleanup");
    }

    private static void joinWithinTimeout(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        assertFalse(thread.isAlive(), "test helper thread should finish");
    }

    private static void assertThreadsWaiting(Thread[] threads) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            boolean allWaiting = true;
            for (Thread thread : threads) {
                Thread.State state = thread.getState();
                if (state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING) {
                    allWaiting = false;
                    break;
                }
            }
            if (allWaiting) {
                return;
            }
            Thread.yield();
        }
        assertTrue(false, "all callers should be waiting for worker termination");
    }

    private static void assertThreadWaiting(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.yield();
        }
        assertTrue(false, "awaitTermination caller should be waiting");
    }
}
