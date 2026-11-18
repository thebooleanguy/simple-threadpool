package dev.thebooleanguy.threadpool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ThreadPool {
    private final BlockingQueue<Runnable> taskQueue;
    private final PoolStateHolder poolStateHolder;
    private final Thread[] workers;
    private final Object lock = new Object();

    public ThreadPool(int queueSize, int threadCount) {
        taskQueue = new ArrayBlockingQueue<Runnable>(queueSize);
        poolStateHolder = new PoolStateHolder(PoolState.RUNNING);
        workers = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            workers[i] = new Thread(new Worker(taskQueue, poolStateHolder, lock));
            workers[i].start();
        }
    }

    public boolean submit(Runnable task) {
        synchronized (lock) {
            if (PoolState.RUNNING == poolStateHolder.getState()) {
                return taskQueue.offer(task);
            } else {
                System.err.println("Pool not running");
                return false;
            }
        }
    }

    public void shutdown() {
        synchronized (lock) {
            poolStateHolder.setState(PoolState.SHUTDOWN);
        }
    }

    public boolean awaitTermination(long time, TimeUnit unit) throws InterruptedException {
        synchronized (lock) {
            if (poolStateHolder.getState() == PoolState.RUNNING) {
                return false;
            } else if (poolStateHolder.getState() == PoolState.TERMINATED) {
                return true;
            }
        }

        if (time == 0) {
            for (int i = 0; i < workers.length; i++) {
                if (workers[i].isAlive()) {
                    return false;
                }
            }
        } else {
            long remainingTime = unit.toNanos(time);
            for (int i = 0; i < workers.length; i++) {
                if (remainingTime <= 0) {
                    return false;
                }

                long millis = remainingTime / 1_000_000;
                long fractionalNanos = remainingTime % 1_000_000;

                long startTime = System.nanoTime();

                workers[i].join(millis, (int) fractionalNanos);

                if (workers[i].isAlive()) {
                    return false;
                }

                long elapsedTime = System.nanoTime() - startTime;
                remainingTime -= elapsedTime;
            }
        }

        synchronized (lock) {
            if (poolStateHolder.getState() == PoolState.SHUTDOWN) {
                poolStateHolder.setState(PoolState.TERMINATED);
                return true;
            } else if (poolStateHolder.getState() == PoolState.TERMINATED) {
                return true;
            }
        }
        return false;
    }
}
