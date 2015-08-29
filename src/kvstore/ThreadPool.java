package kvstore;

import java.util.ArrayList;
import java.util.List;

public class ThreadPool {

  /* Array of threads in the threadpool */
  public Thread threads[];
  
  private final List<Runnable> jobQueue;
  private boolean stopped;
  private final int poolSize;

  /**
   * Constructs a Threadpool with a certain number of threads.
   *
   * @param size number of threads in the thread pool
   */
  public ThreadPool(int size) {
    threads = new Thread[size];
    jobQueue = new ArrayList<>();
    stopped = false;
    poolSize = size;

    for (int i = 0; i < size; i++) {
      threads[i] = new WorkerThread(this);
    }
    // Starting workers
    for (int i = 0; i < size; i++) {
      threads[i].start();
    }
  }

  public synchronized void stopThreadPool() {
    stopped = true;
    for (int i = 0; i < poolSize; i++) {
      threads[i].interrupt();
    }
  }

  /**
   * Add a job to the queue of jobs that have to be executed. As soon as a
   * thread is available, the thread will retrieve a job from this queue if
   * if one exists and start processing it.
   *
   * @param r job that has to be executed
   *
   * @throws InterruptedException if thread is interrupted while in blocked
   *                              state. Your implementation may or may not actually throw this.
   */
  public void addJob(Runnable r) throws InterruptedException {
    synchronized (jobQueue) {
      jobQueue.add(r);
      jobQueue.notifyAll();
    }
  }

  /**
   * Block until a job is present in the queue and retrieve the job
   *
   * @return A runnable task that has to be executed
   *
   * @throws InterruptedException if thread is interrupted while in blocked
   *                              state. Your implementation may or may not actually throw this.
   */
  public Runnable getJob() throws InterruptedException {
    synchronized (jobQueue) {
      while (jobQueue.isEmpty()) {
        jobQueue.wait();       
      }
       return jobQueue.remove(0);
    }    
  }

  /**
   * A thread in the thread pool.
   */
  public class WorkerThread extends Thread {

    public ThreadPool threadPool;
    private boolean stopped;

    /**
     * Constructs a thread for this particular ThreadPool.
     *
     * @param pool the ThreadPool containing this thread
     */
    public WorkerThread(ThreadPool pool) {
      threadPool = pool;
      stopped = false;
    }

    /**
     * Scan for and execute tasks.
     */
    @Override
    public void run() {
      while (!stopped) {
        try {
          threadPool.getJob().run();
        } catch (Exception e) {
          System.out.println(e);
        }
      }
    }

    public synchronized void stopWorker() {
      stopped = true;
      interrupt();
    }
  }

}
