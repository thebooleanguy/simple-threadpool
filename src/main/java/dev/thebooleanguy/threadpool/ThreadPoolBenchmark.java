package dev.thebooleanguy.threadpool;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public final class ThreadPoolBenchmark {
    private static final long ROUND_TIMEOUT_SECONDS = 300;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;
    private static final long RETRY_PAUSE_NANOS = 100_000;
    private static final long BLOCKING_MILLIS = 5;

    private ThreadPoolBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        if (config == null) {
            return;
        }

        System.out.printf("Workers: %d, queue capacity: %d, tasks: %d, producers: %d, warmup tasks: %d%n",
                config.workerCount, config.queueCapacity, config.taskCount,
                config.producerCount, config.warmupTaskCount);
        System.out.println(
                "Executors.newFixedThreadPool uses an external outstanding-task limit of workers + queue capacity.");
        System.out.println("Retries count temporary queue-full rejections; task failures are reported separately.");
        printHeader();

        Scenario[] scenarios = {
                new Scenario("Basic CPU", taskId -> calculate(taskId, 64), 1),
                new Scenario("Concurrent submit", taskId -> calculate(taskId, 64), config.producerCount),
                new Scenario("CPU-intensive", taskId -> calculate(taskId, 20_000), 1),
                new Scenario("Blocking (5 ms)", taskId -> {
                    Thread.sleep(BLOCKING_MILLIS);
                    return taskId;
                }, 1)
        };

        for (Scenario scenario : scenarios) {
            for (PoolKind poolKind : PoolKind.values()) {
                BenchmarkPool pool = createPool(poolKind, config);
                try {
                    int warmupCount = Math.min(config.warmupTaskCount, config.taskCount);
                    if (warmupCount > 0) {
                        runRound(pool, scenario.workload, warmupCount, scenario.producerCount);
                    }
                    RoundResult result = runRound(pool, scenario.workload,
                            config.taskCount, scenario.producerCount);
                    printResult(scenario, poolKind, config.taskCount, result);
                } finally {
                    shutdownAndAwait(pool);
                }
            }
        }
    }

    private static BenchmarkPool createPool(PoolKind kind, Config config) {
        return switch (kind) {
            case CUSTOM -> new CustomPool(new ThreadPool(config.queueCapacity, config.workerCount));
            case FIXED_EXECUTOR -> new ExecutorPool(
                    Executors.newFixedThreadPool(config.workerCount),
                    new Semaphore(Math.addExact(config.workerCount, config.queueCapacity)));
            case BOUNDED_EXECUTOR -> new ExecutorPool(new ThreadPoolExecutor(
                    config.workerCount,
                    config.workerCount,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(config.queueCapacity),
                    new ThreadPoolExecutor.AbortPolicy()), null);
        };
    }

    private static RoundResult runRound(BenchmarkPool pool, Workload workload,
            int taskCount, int producerCount) throws InterruptedException {
        RoundMetrics metrics = new RoundMetrics(taskCount);
        CountDownLatch producersReady = new CountDownLatch(producerCount);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        Thread[] producers = new Thread[producerCount];
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(ROUND_TIMEOUT_SECONDS);

        for (int producerIndex = 0; producerIndex < producerCount; producerIndex++) {
            int index = producerIndex;
            producers[index] = new Thread(() -> {
                producersReady.countDown();
                try {
                    start.await();
                    submitTasks(pool, workload, metrics, index, producerCount, taskCount, deadline);
                } catch (Throwable failure) {
                    producerFailure.compareAndSet(null, failure);
                }
            }, "thread-pool-benchmark-producer-" + index);
            producers[index].start();
        }

        boolean roundFinished = false;
        try {
            if (!producersReady.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Producer threads did not become ready");
            }
            long startNanos = System.nanoTime();
            start.countDown();

            for (Thread producer : producers) {
                joinUntil(producer, deadline);
            }

            Throwable failure = producerFailure.get();
            if (failure != null) {
                throw new IllegalStateException("A producer failed", failure);
            }

            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0 || !metrics.completed.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("Timed out waiting for benchmark tasks to complete");
            }

            long elapsedNanos = System.nanoTime() - startNanos;
            roundFinished = true;
            return new RoundResult(elapsedNanos, metrics);
        } finally {
            start.countDown();
            if (!roundFinished) {
                for (Thread producer : producers) {
                    if (producer.isAlive()) {
                        producer.interrupt();
                    }
                }
            }
        }
    }

    private static void submitTasks(BenchmarkPool pool, Workload workload, RoundMetrics metrics,
            int producerIndex, int producerCount, int taskCount, long deadline) throws Exception {
        for (int taskId = producerIndex; taskId < taskCount; taskId += producerCount) {
            int currentTaskId = taskId;
            Runnable task = () -> {
                long taskStart = System.nanoTime();
                metrics.executed.increment();
                try {
                    metrics.checksum.add(workload.run(currentTaskId));
                } catch (Exception failure) {
                    metrics.failures.increment();
                } finally {
                    metrics.executionNanos.add(System.nanoTime() - taskStart);
                    metrics.completed.countDown();
                }
            };

            while (!pool.submit(task)) {
                metrics.rejectionAttempts.increment();
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Timed out retrying a rejected task");
                }
                LockSupport.parkNanos(RETRY_PAUSE_NANOS);
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Producer interrupted while retrying submission");
                }
            }
        }
    }

    private static void joinUntil(Thread thread, long deadline) throws InterruptedException {
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new IllegalStateException("Timed out waiting for producer threads");
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
        int nanos = (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(millis));
        thread.join(millis, nanos);
        if (thread.isAlive()) {
            throw new IllegalStateException("Timed out waiting for producer threads");
        }
    }

    private static void shutdownAndAwait(BenchmarkPool pool) throws InterruptedException {
        pool.shutdown();
        if (!pool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Executor did not terminate within the shutdown timeout");
        }
    }

    private static long calculate(int seed, int rounds) {
        long value = 0x9E3779B97F4A7C15L ^ seed;
        for (int i = 0; i < rounds; i++) {
            value ^= value >>> 12;
            value ^= value << 25;
            value ^= value >>> 27;
            value *= 0x2545F4914F6CDD1DL;
        }
        return value;
    }

    private static void printHeader() {
        System.out.printf("%-18s %-22s %7s %9s %11s %10s %7s %8s %12s%n",
                "Scenario", "Pool", "Tasks", "Total(s)", "Avg run(ms)",
                "Tasks/s", "Failed", "Retries", "Checksum");
        System.out.println("-".repeat(112));
    }

    private static void printResult(Scenario scenario, PoolKind poolKind,
            int taskCount, RoundResult result) {
        long executed = result.metrics.executed.sum();
        double averageMillis = executed == 0 ? 0
                : result.metrics.executionNanos.sum() / (double) executed / 1_000_000.0;
        double elapsedSeconds = result.elapsedNanos / 1_000_000_000.0;
        double tasksPerSecond = elapsedSeconds == 0 ? 0 : taskCount / elapsedSeconds;

        System.out.printf(Locale.ROOT, "%-18s %-22s %7d %9.3f %11.3f %10.0f %7d %8d %12d%n",
                scenario.name, poolKind.displayName, taskCount, elapsedSeconds,
                averageMillis, tasksPerSecond, result.metrics.failures.sum(),
                result.metrics.rejectionAttempts.sum(), result.metrics.checksum.sum());
    }

    private interface BenchmarkPool {
        boolean submit(Runnable task);

        void shutdown();

        boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
    }

    private static final class CustomPool implements BenchmarkPool {
        private final ThreadPool pool;

        private CustomPool(ThreadPool pool) {
            this.pool = pool;
        }

        @Override
        public boolean submit(Runnable task) {
            return pool.submit(task);
        }

        @Override
        public void shutdown() {
            pool.shutdown();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return pool.awaitTermination(timeout, unit);
        }
    }

    private static final class ExecutorPool implements BenchmarkPool {
        private final ExecutorService executor;
        private final Semaphore outstandingLimit;

        private ExecutorPool(ExecutorService executor, Semaphore outstandingLimit) {
            this.executor = executor;
            this.outstandingLimit = outstandingLimit;
        }

        @Override
        public boolean submit(Runnable task) {
            if (outstandingLimit != null && !outstandingLimit.tryAcquire()) {
                return false;
            }
            Runnable submittedTask = outstandingLimit == null ? task : () -> {
                try {
                    task.run();
                } finally {
                    outstandingLimit.release();
                }
            };
            try {
                executor.execute(submittedTask);
                return true;
            } catch (RejectedExecutionException rejected) {
                if (outstandingLimit != null) {
                    outstandingLimit.release();
                }
                return false;
            }
        }

        @Override
        public void shutdown() {
            executor.shutdown();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return executor.awaitTermination(timeout, unit);
        }
    }

    private static final class RoundMetrics {
        private final CountDownLatch completed;
        private final LongAdder executed = new LongAdder();
        private final LongAdder failures = new LongAdder();
        private final LongAdder rejectionAttempts = new LongAdder();
        private final LongAdder executionNanos = new LongAdder();
        private final LongAdder checksum = new LongAdder();

        private RoundMetrics(int taskCount) {
            completed = new CountDownLatch(taskCount);
        }
    }

    private record RoundResult(long elapsedNanos, RoundMetrics metrics) {
    }

    private record Scenario(String name, Workload workload, int producerCount) {
    }

    private enum PoolKind {
        CUSTOM("Custom ThreadPool"),
        FIXED_EXECUTOR("Executors fixed pool"),
        BOUNDED_EXECUTOR("ThreadPoolExecutor");

        private final String displayName;

        PoolKind(String displayName) {
            this.displayName = displayName;
        }
    }

    @FunctionalInterface
    private interface Workload {
        long run(int taskId) throws Exception;
    }

    private static final class Config {
        private int workerCount = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors(), 8));
        private int queueCapacity = 1024;
        private int taskCount = 10_000;
        private int producerCount = Math.max(2, Math.min(workerCount, 8));
        private int warmupTaskCount = 500;

        private static Config parse(String[] args) {
            Config config = new Config();
            for (int i = 0; i < args.length; i++) {
                String option = args[i];
                if ("--help".equals(option)) {
                    printUsage();
                    return null;
                }
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for " + option);
                }
                int value;
                try {
                    value = Integer.parseInt(args[++i]);
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException("Expected an integer for " + option, exception);
                }
                switch (option) {
                    case "--workers" -> config.workerCount = value;
                    case "--queue-capacity" -> config.queueCapacity = value;
                    case "--tasks" -> config.taskCount = value;
                    case "--producers" -> config.producerCount = value;
                    case "--warmup" -> config.warmupTaskCount = value;
                    default -> throw new IllegalArgumentException("Unknown option: " + option);
                }
            }
            if (config.workerCount <= 0 || config.queueCapacity <= 0
                    || config.taskCount <= 0 || config.producerCount <= 0
                    || config.warmupTaskCount < 0) {
                throw new IllegalArgumentException(
                        "Workers, queue capacity, tasks and producers must be positive; warmup cannot be negative");
            }
            Math.addExact(config.workerCount, config.queueCapacity);
            return config;
        }

        private static void printUsage() {
            System.out.println("Usage: ThreadPoolBenchmark [--workers N] [--queue-capacity N] [--tasks N] "
                    + "[--producers N] [--warmup N]");
        }
    }
}