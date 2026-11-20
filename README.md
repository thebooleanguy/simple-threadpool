# Simple Thread Pool

A small fixed-size Java thread pool with a bounded task queue. Submit `Runnable` tasks, then shut the pool down gracefully when you are done.

## Requirements

- Java 17 or newer
- Maven

## Use It

```java
import dev.thebooleanguy.threadpool.ThreadPool;
import java.util.concurrent.TimeUnit;

public class Example {
    public static void main(String[] args) throws InterruptedException {
        ThreadPool pool = new ThreadPool(16, 4); // queue capacity, worker count

        for (int i = 0; i < 10; i++) {
            int taskNumber = i;
            boolean accepted = pool.submit(() ->
                    System.out.println("Running task " + taskNumber));

            if (!accepted) {
                System.out.println("Task " + taskNumber + " was not accepted");
            }
        }

        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            System.out.println("Pool did not terminate within 30 seconds");
        }
    }
}
```

`submit` is non-blocking. It returns `false` if the bounded queue is full or the pool has been shut down. Call `shutdown` to stop accepting new tasks; tasks already accepted are allowed to finish. `awaitTermination` waits up to the given timeout and returns whether the workers have stopped.

## Build and Test

```sh
mvn test
```

The tests cover concurrent execution, queue capacity, shutdown behavior, and worker recovery after a task throws a runtime exception.