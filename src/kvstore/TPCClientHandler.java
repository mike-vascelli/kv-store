package kvstore;

import java.net.Socket;
import static kvstore.KVConstants.ERROR_INVALID_FORMAT;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.SUCCESS;
import static kvstore.KVConstants.TIMEOUT;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * It uses a threadPool to ensure that none of it's methods are blocking.
 */
public class TPCClientHandler implements NetworkHandler {

  public TPCMaster tpcMaster;
  public ThreadPool threadPool;

  /**
   * Constructs a TPCClientHandler with ThreadPool of a single thread.
   *
   * @param tpcMaster TPCMaster to carry out requests
   */
  public TPCClientHandler(TPCMaster tpcMaster) {
    this(tpcMaster, 1);
  }

  /**
   * Constructs a TPCClientHandler with ThreadPool of a single thread.
   *
   * @param tpcMaster   TPCMaster to carry out requests
   * @param connections number of threads in threadPool to service requests
   */
  public TPCClientHandler(TPCMaster tpcMaster, int connections) {   
    this.tpcMaster = tpcMaster;
    threadPool = new ThreadPool(connections); 
  }

  /**
   * Creates a job to service the request on a socket and enqueues that job
   * in the thread pool. Ignore InterruptedExceptions.
   *
   * @param client Socket connected to the client with the request
   */
  @Override
  public void handle(Socket client) { 
    try {
      KVMessage request = new KVMessage(client, TIMEOUT);

      switch (request.getMsgType()) {
        case KVConstants.GET_REQ:
          threadPool.addJob(new getJob(request, client));
          break;
        case KVConstants.PUT_REQ:
        case KVConstants.DEL_REQ:
          threadPool.addJob(new TPCJob(request, client));
          break;
        default:
          throw new KVException(ERROR_INVALID_FORMAT);
      }
    } catch (KVException e) {
      //System.out.println("CLIENT HANDLER HANDLE 1" + e);
      try {
        e.getKVMessage().sendMessage(client);
      } catch (KVException ex) {
        System.out.println("CLIENT HANDLER HANDLE 2" + ex);
      }

    } catch (InterruptedException ex) {
      System.out.println("CLIENT HANDLER HANDLE 3" + ex);
    }
  
  }


  private class getJob implements Runnable {

    KVMessage request;
    Socket client;

    getJob(KVMessage request, Socket client) {
      this.request = request;
      this.client = client;
    }

    @Override
    public void run() {
      try {
        String value = tpcMaster.handleGet(request);
        KVMessage response = new KVMessage(RESP);
        response.setKey(request.getKey());
        response.setValue(value);
        response.sendMessage(client);
      } catch (KVException e1) {
        //System.out.println("CLIENT HANDLER HANDLE GET 1" + e1);
        try {
          e1.getKVMessage().sendMessage(client);
        } catch (KVException e2) {
          System.out.println("CLIENT HANDLER HANDLE GET 2" + e2);
        }
      }
    }
  }

  private class TPCJob implements Runnable {

    KVMessage request;
    Socket client;

    TPCJob(KVMessage request, Socket client) {
      this.request = request;
      this.client = client;
    }

    @Override
    public void run() {
      try {
        tpcMaster.handleTPCRequest(request, request.getMsgType().equals(PUT_REQ));
        KVMessage response = new KVMessage(RESP, SUCCESS);
        response.sendMessage(client);
      } catch (KVException e1) {
       // System.out.println("CLIENT HANDLER HANDLE TCP REQUEST 1" + e1);
        try {
          e1.getKVMessage().sendMessage(client);
        } catch (KVException e2) {
          System.out.println("CLIENT HANDLER HANDLE TCP REQUEST 2" + e2);
        } catch (Exception e) {
          System.out.println("CLIENT HANDLER HANDLE TCP REQUEST 3 " + e);
        }
      }
    }
  }
    
}
