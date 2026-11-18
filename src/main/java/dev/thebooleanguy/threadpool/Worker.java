package dev.thebooleanguy.threadpool;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Worker implements Runnable {
    private final BlockingQueue<Runnable> taskQueue;
    private final PoolStateHolder poolStateHolder;
    private final Object lock;

    public Worker(BlockingQueue<Runnable> taskQueue, PoolStateHolder poolStateHolder, Object lock) {
        this.taskQueue = taskQueue;
        this.poolStateHolder = poolStateHolder;
        this.lock = lock;
    }

    @Override
    public void run() {
        while (true) {
            Runnable task = null;
            try {

                task = taskQueue.poll(1000, TimeUnit.MILLISECONDS);

            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
                Thread.currentThread().interrupt();
                return;
            }
            try {
                if (task != null) {
                    task.run();
                } else {
                    if (PoolState.RUNNING == readPoolState()) {
                        continue;
                    } else {
                        break;
                    }
                }
            } catch (RuntimeException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    private PoolState readPoolState() {
        synchronized (lock) {
            return poolStateHolder.getState();
        }
    }
}
