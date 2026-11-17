package dev.thebooleanguy.threadpool;

import java.lang.Math;

public class Task implements Runnable {

    @Override
    public void run() {
        double taskNo = Math.floor(Math.random() * (100 - 0 + 1));

        if (taskNo % 2 == 0) {
            throw new RuntimeException("Task " + taskNo + " failed to run");
        } else {
            System.out.println("Task " + taskNo);
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
            Thread.currentThread().interrupt();
        }

    }
}
