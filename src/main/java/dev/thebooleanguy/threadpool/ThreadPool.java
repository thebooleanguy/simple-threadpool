package dev.thebooleanguy.threadpool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ThreadPool {
    private final BlockingQueue<Runnable> taskQueue;
    private final PoolStateHolder poolStateHolder;
    private final Thread[] workers;

    public ThreadPool(int queueSize, int threadCount) {
        taskQueue = new ArrayBlockingQueue<Runnable>(queueSize);
        workers = new Thread[threadCount];
        poolStateHolder = new PoolStateHolder(PoolState.RUNNING);

        for (int i = 0; i < threadCount; i++) {
            workers[i] = new Thread(new Worker(taskQueue, poolStateHolder));
            workers[i].start();
        }
    }
}
