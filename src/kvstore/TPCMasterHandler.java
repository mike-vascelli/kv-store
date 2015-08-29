package kvstore;

import java.io.IOException;
import java.net.Socket;
import static kvstore.KVConstants.ABORT;
import static kvstore.KVConstants.ACK;
import static kvstore.KVConstants.COMMIT;
import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.ERROR_COULD_NOT_CONNECT;
import static kvstore.KVConstants.ERROR_COULD_NOT_CREATE_SOCKET;
import static kvstore.KVConstants.ERROR_INVALID_FORMAT;
import static kvstore.KVConstants.ERROR_NO_SUCH_KEY;
import static kvstore.KVConstants.ERROR_OVERSIZED_KEY;
import static kvstore.KVConstants.ERROR_OVERSIZED_VALUE;
import static kvstore.KVConstants.ERROR_SLAVE_QUOTA_ALREADY_MET;
import static kvstore.KVConstants.GET_REQ;
import static kvstore.KVConstants.MAX_KEY_SIZE;
import static kvstore.KVConstants.MAX_VAL_SIZE;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.READY;
import static kvstore.KVConstants.REGISTER;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.TIMEOUT;

/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

  static final int REGISTRATION_PORT = 9090;
  public long slaveID;
  public KVServer kvServer;
  public TPCLog tpcLog;
  public ThreadPool threadpool;

  /**
   * Constructs a TPCMasterHandler with one connection in its ThreadPool
   *
   * @param slaveID  the ID for this slave server
   * @param kvServer KVServer for this slave
   * @param log      the log for this slave
   */
  public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
    this(slaveID, kvServer, log, 1);
  }

  /**
   * Constructs a TPCMasterHandler with a variable number of connections
   * in its ThreadPool
   *
   * @param slaveID     the ID for this slave server
   * @param kvServer    KVServer for this slave
   * @param log         the log for this slave
   * @param connections the number of connections in this slave's ThreadPool
   */
  public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
    this.slaveID = slaveID;
    this.kvServer = kvServer;
    this.tpcLog = log;
    this.threadpool = new ThreadPool(connections);
  }

  /**
   * Registers this slave server with the master.
   *
   * @param masterHostname
   * @param server         SocketServer used by this slave server (which contains the
   *                       hostname and port this slave is listening for requests on
   *
   * @throws KVException with ERROR_INVALID_FORMAT if the response from the
   *                     master is received and parsed but does not correspond to a
   *                     success as defined in the spec OR any other KVException such
   *                     as those expected in KVClient in project 3 if unable to receive
   *                     and/or parse message
   */
  public void registerWithMaster(String masterHostname, SocketServer server)
          throws KVException {  
    try {
      try (Socket sock = new Socket(masterHostname, REGISTRATION_PORT)) {
        KVMessage request = new KVMessage(REGISTER);
        String requestMessage = slaveID + "@" + server.getHostname() + ":" + server.getPort();
        request.setMessage(requestMessage);
        request.sendMessage(sock);

        KVMessage response = new KVMessage(sock, TIMEOUT);

        if (response.getMessage() == null || !response.getMsgType().equals(RESP)
            || !response.getMessage().equals("Successfully registered " + requestMessage)) {
          sock.close();
          throw new KVException(ERROR_INVALID_FORMAT);
        } else if (response.getMessage() != null
                   && response.getMessage().equals(ERROR_SLAVE_QUOTA_ALREADY_MET)) {
          throw new KVException(ERROR_SLAVE_QUOTA_ALREADY_MET);
        }
      }
    } catch (IOException e) {
      throw new KVException(ERROR_COULD_NOT_CONNECT);
    } catch (IllegalArgumentException e) {
      throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
    }    
  }

  /**
   * Creates a job to service the request on a socket and enqueues that job
   * in the thread pool. Ignore any InterruptedExceptions.
   *
   * @param master Socket connected to the master with the request
   */
  @Override
  public void handle(Socket master) {  
    try {
      KVMessage request = new KVMessage(master, TIMEOUT);

      switch (request.getMsgType()) {
        case GET_REQ:
          threadpool.addJob(new getJob(request.getKey(), master));
          break;
        case PUT_REQ:
          phase1(request, PUT_REQ, master);
          break;
        case DEL_REQ:
          phase1(request, DEL_REQ, master);
          break;
        case COMMIT:
          phase2(request, COMMIT, master);
          break;
        case ABORT:
          phase2(request, ABORT, master);
          break;
        default:
          throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
      }
    } catch (KVException e) {
      System.out.println("MASTER HANDLER HANDLE 1" + e);
      try {
        e.getKVMessage().sendMessage(master);
      } catch (KVException ex) {
        System.out.println("MASTER HANDLER HANDLE 2" + ex);
      }

    } catch (InterruptedException ex) {
      System.out.println("MASTER HANDLER HANDLE 3" + ex);
    }    
  }

 
  private void phase1(KVMessage request, String requestType, Socket master) throws KVException {
    if (requestType.equals(PUT_REQ)) {
      tpcLog.appendAndFlush(request);
      if (request.getKey().length() > MAX_KEY_SIZE) {
        new KVMessage(ABORT, ERROR_OVERSIZED_KEY).sendMessage(master);
      } else if (request.getValue().length() > MAX_VAL_SIZE) {
        new KVMessage(ABORT, ERROR_OVERSIZED_VALUE).sendMessage(master);
      } else {
        new KVMessage(READY).sendMessage(master);
      }
    } else if (requestType.equals(DEL_REQ)) {
      tpcLog.appendAndFlush(request);
      String key = request.getKey();
      if (key.isEmpty() || !kvServer.hasKey(key)) {
        new KVMessage(ABORT, ERROR_NO_SUCH_KEY).sendMessage(master);
      } else {
        new KVMessage(READY).sendMessage(master);
      }
    }
  }

  private void phase2(KVMessage request, String requestType, Socket master)
          throws KVException, InterruptedException {
    if (requestType.equals(COMMIT)) {
      final KVMessage requestToCommit = tpcLog.getLastEntry();    
      switch (requestToCommit.getMsgType()) {
        case PUT_REQ:
          threadpool.addJob(new Runnable() {
            @Override
            public void run() {
              try {
                kvServer.put(requestToCommit.getKey(), requestToCommit.getValue());               
              } catch (KVException e1) {
                System.out.println("MASTER HANDLER PUT" + e1);
              }
            }
          });
          break;
        case DEL_REQ:          
          threadpool.addJob(new Runnable() {
            @Override
            public void run() {
              try {
                kvServer.del(requestToCommit.getKey());
              } catch (KVException e1) {
                System.out.println("MASTER HANDLER DEL " + e1);
              }
            }
          });
          break;
        default:       
          // The requested operation was already committed
          // No need to log another commit. Just send ACK
          if (requestToCommit.getMsgType().equals(COMMIT)) {
            new KVMessage(ACK).sendMessage(master);
            return;
          }
      }
      tpcLog.appendAndFlush(request);
      new KVMessage(ACK).sendMessage(master);
    } else if (requestType.equals(ABORT)) {     
      tpcLog.appendAndFlush(request);
      new KVMessage(ACK).sendMessage(master);
    }
  }
 
  
  private class getJob implements Runnable {

    String key;
    Socket master;

    getJob(String key, Socket master) {
      this.key = key;
      this.master = master;
    }

    @Override
    public void run() {
      try {
        String value = kvServer.get(key);
        KVMessage response = new KVMessage(RESP);
        response.setKey(key);
        response.setValue(value);
        response.sendMessage(master);
      } catch (KVException e1) {
        try {
          e1.getKVMessage().sendMessage(master);
        } catch (KVException e2) {
          System.out.println(e2);
        }
      }
    }
  }
    
}
