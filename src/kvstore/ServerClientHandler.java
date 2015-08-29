package kvstore;

import java.net.Socket;
import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.ERROR_INVALID_FORMAT;
import static kvstore.KVConstants.GET_REQ;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.SUCCESS;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class ServerClientHandler implements NetworkHandler {

  public KVServer kvServer;
  public ThreadPool threadPool;

  /**
   * Constructs a ServerClientHandler with ThreadPool of a single thread.
   *
   * @param kvServer KVServer to carry out requests
   */
  public ServerClientHandler(KVServer kvServer) {
    this(kvServer, 1);
  }

  /**
   * Constructs a ServerClientHandler with ThreadPool of thread equal to
   * the number passed in as connections.
   *
   * @param kvServer    KVServer to carry out requests
   * @param connections number of threads in threadPool to service requests
   */
  public ServerClientHandler(KVServer kvServer, int connections) {
    this.kvServer = kvServer;
    threadPool = new ThreadPool(connections);
  }

  /**
   * Creates a job to service the request for a socket and enqueues that job
   * in the thread pool. Ignore any InterruptedExceptions.
   *
   * @param client Socket connected to the client with the request
   */
  @Override
  public void handle(Socket client) {
    try {
      KVMessage request = new KVMessage(client);

      switch (request.getMsgType()) {
        case GET_REQ:
          threadPool.addJob(new getJob(request.getKey(), client));
          break;
        case PUT_REQ:
          threadPool.addJob(new putJob(request.getKey(), request.getValue(), client));
          break;
        case DEL_REQ:
          threadPool.addJob(new delJob(request.getKey(), client));
          break;
        default:
          throw new KVException(ERROR_INVALID_FORMAT);
      }
    } catch (KVException e) {
      System.out.println("SERVCLIENT HANDLE 1" + e);
      try {
        e.getKVMessage().sendMessage(client);
      } catch (KVException ex) {
        System.out.println("SERVCLIENT HANDLE 2" + ex);
      }

    } catch (InterruptedException ex) {
      System.out.println("SERVCLIENT HANDLE 3" + ex);
    }
  }

  private class getJob implements Runnable {

    String key;
    Socket client;

    getJob(String key, Socket client) {
      this.key = key;
      this.client = client;
    }

    @Override
    public void run() {
      try {
        String value = kvServer.get(key);
        KVMessage response = new KVMessage(RESP);
        response.setKey(key);
        response.setValue(value);
        response.sendMessage(client);
      } catch (KVException e1) {
        try {
          e1.getKVMessage().sendMessage(client);
        } catch (KVException e2) {
        }
      }
    }
  }

  private class putJob implements Runnable {

    String key;
    String value;
    Socket client;

    putJob(String key, String value, Socket client) {
      this.key = key;
      this.value = value;
      this.client = client;
    }

    @Override
    public void run() {
      try {
        kvServer.put(key, value);
        KVMessage response = new KVMessage(RESP, SUCCESS);
        response.sendMessage(client);
      } catch (KVException e1) {
        System.out.println("SERVCLIENT PUT 1" + e1);
        try {

          e1.getKVMessage().sendMessage(client);
        } catch (KVException e2) {
          System.out.println("SERVCLIENT PUT 2" + e2);
        } catch (Exception e) {
          System.out.println("SERVCLIENT PUT 3 " + e);
        }
      }
    }
  }

  private class delJob implements Runnable {

    String key;
    Socket client;

    delJob(String key, Socket client) {
      this.key = key;
      this.client = client;
    }

    @Override
    public void run() {
      try {
        kvServer.del(key);
        KVMessage response = new KVMessage(RESP, SUCCESS);
        response.sendMessage(client);
      } catch (KVException e1) {
        try {
          e1.getKVMessage().sendMessage(client);
        } catch (KVException e2) {
        }
      }
    }
  }

}
