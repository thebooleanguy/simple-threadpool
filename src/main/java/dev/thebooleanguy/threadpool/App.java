package dev.thebooleanguy.threadpool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class App {
    public static void main(String[] args) {

        BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(10);
        for (int i = 0; i < 10; i++) {
            taskQueue.add(new Task());
        }

        Thread workerThreadA = new Thread(new Worker(taskQueue));
        Thread workerThreadB = new Thread(new Worker(taskQueue));
        Thread workerThreadC = new Thread(new Worker(taskQueue));
        workerThreadA.start();
        workerThreadB.start();
        workerThreadC.start();
    }
}
