package dev.thebooleanguy.threadpool;

import java.util.concurrent.BlockingQueue;

public class Worker implements Runnable {
    private final BlockingQueue<Runnable> taskQueue;
    private final PoolStateHolder poolStateHolder;

    public Worker(BlockingQueue<Runnable> taskQueue, PoolStateHolder poolStateHolder) {
        this.taskQueue = taskQueue;
        this.poolStateHolder = poolStateHolder;
    }

    @Override
    public void run() {
        while (true) {
            Runnable task = new Task();
            try {
                task = taskQueue.take();
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
                Thread.currentThread().interrupt();
                return;
            }
            try {

                task.run();
            } catch (RuntimeException e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
