package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class TPCRegistrationHandler implements NetworkHandler {

  private ThreadPool threadpool;
  private TPCMaster master;

  /**
   * Constructs a TPCRegistrationHandler with a ThreadPool of a single thread.
   *
   * @param master TPCMaster to register slave with
   */
  public TPCRegistrationHandler(TPCMaster master) {
    this(master, 1);
  }

  /**
   * Constructs a TPCRegistrationHandler with ThreadPool of thread equal to the
   * number given as connections.
   *
   * @param master      TPCMaster to carry out requests
   * @param connections number of threads in threadPool to service requests
   */
  public TPCRegistrationHandler(TPCMaster master, int connections) {
    this.threadpool = new ThreadPool(connections);
    this.master = master;
  }

  /**
   * Creates a job to service the request on a socket and enqueues that job
   * in the thread pool. Ignore any InterruptedExceptions.
   *
   * @param slave Socket connected to the slave with the request
   */
  @Override
  public void handle(Socket slave) {  
    try {
      KVMessage request = new KVMessage(slave, TIMEOUT);

      if (request.getMsgType().equals(REGISTER)) {
        threadpool.addJob(new registrationJob(request.getMessage(), slave));
      } else {
        throw new KVException(ERROR_INVALID_FORMAT);
      }
    } catch (KVException e) {
      System.out.println("REG HANDLE 1" + e);
      try {
        e.getKVMessage().sendMessage(slave);
      } catch (KVException ex) {
        System.out.println("REG HANDLE 2" + ex);
      }

    } catch (InterruptedException ex) {
      System.out.println("REG HANDLE 3" + ex);
    }   
  }

  
  private class registrationJob implements Runnable {

    String slaveInfo;
    Socket slaveSocket;

    registrationJob(String info, Socket slave) {
      slaveInfo = info;
      slaveSocket = slave;
    }

    @Override
    public void run() {      
      try {
        TPCSlaveInfo slave = new TPCSlaveInfo(slaveInfo);
        master.registerSlave(slave);
        KVMessage response = new KVMessage(RESP);
        response.setMessage("Successfully registered " + slaveInfo);
        response.sendMessage(slaveSocket);
      } catch (KVException e1) {      
        try {
          e1.getKVMessage().sendMessage(slaveSocket);
        } catch (KVException e2) {
          System.out.println(e2);
        }
      }
    }
  } 

}
