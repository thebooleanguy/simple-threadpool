package dev.thebooleanguy.threadpool;

import java.util.concurrent.TimeUnit;

// An entry-point for demonstration purposes
public class App {
    public static void main(String[] args) {
        ThreadPool threadPool = new ThreadPool(10, 3);

        for (int i = 0; i < 3; i++) {
            threadPool.submit(new Task());
        }

        threadPool.shutdown();

        try {
            System.out.println(threadPool.awaitTermination(500, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {

        }

    }
}
