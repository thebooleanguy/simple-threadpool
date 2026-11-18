package dev.thebooleanguy.threadpool;

import java.lang.Math;

public class Task implements Runnable {

    @Override
    public void run() {
        double taskNo = Math.floor(Math.random() * (100 - 0 + 1));
        String workerName = Thread.currentThread().getName();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            System.err.println("Task " + taskNo + ", Worker " + workerName + e.getMessage());
            Thread.currentThread().interrupt();
        }

        if (taskNo % 2 == 0) {
            throw new RuntimeException("Task " + taskNo + " failed to run on " +
                    workerName);
        } else {
            System.out.println("Task " + taskNo + " running on " + workerName);
        }
    }
}
