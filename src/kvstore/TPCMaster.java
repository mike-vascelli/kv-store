package kvstore;

import java.net.Socket;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import static kvstore.KVConstants.ABORT;
import static kvstore.KVConstants.ACK;
import static kvstore.KVConstants.COMMIT;
import static kvstore.KVConstants.ERROR_INVALID_FORMAT;
import static kvstore.KVConstants.ERROR_NO_SUCH_KEY;
import static kvstore.KVConstants.ERROR_SLAVE_QUOTA_ALREADY_MET;
import static kvstore.KVConstants.READY;
import static kvstore.KVConstants.RESP;

public class TPCMaster {

  public static final int TIMEOUT = 3000;
  public static final int MIN_SLAVE_NUM = 2;
  public final int numSlaves;
  public final KVCache masterCache; 
  private final TreeMap<Long, TPCSlaveInfo> slaveMap;
  
  /**
   * Creates TPCMaster, expecting numSlaves slave servers to eventually register
   *
   * @param numSlaves number of slave servers expected to register
   * @param cache     KVCache to cache results on master
   */
  public TPCMaster(int numSlaves, KVCache cache) {
    // Minimum allowed num of slaves must be 2
    if (numSlaves < MIN_SLAVE_NUM) {
      numSlaves = MIN_SLAVE_NUM;
    }
    this.numSlaves = numSlaves;
    this.masterCache = cache;   
    slaveMap = new TreeMap<>(new keyComparator()); 
  }

  /**
   * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
   * adapted from String.hashCode().
   *
   * @param string String to hash to 64-bit
   *
   * @return long hashcode
   */
  public static long hashTo64bit(String string) {
    long h = 1125899906842597L;
    int len = string.length();

    for (int i = 0; i < len; i++) {
      h = (31 * h) + string.charAt(i);
    }
    return h;
  }

  /**
   * Compares two longs as if they were unsigned (Java doesn't have unsigned
   * data types except for char). Borrowed from http://goo.gl/QyuI0V
   *
   * @param n1 First long
   * @param n2 Second long
   *
   * @return is unsigned n1 less than unsigned n2
   */
  public static boolean isLessThanUnsigned(long n1, long n2) {
    return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
  }

  /**
   * Compares two longs as if they were unsigned, uses isLessThanUnsigned
   *
   * @param n1 First long
   * @param n2 Second long
   *
   * @return is unsigned n1 less than or equal to unsigned n2
   */
  public static boolean isLessThanEqualUnsigned(long n1, long n2) {
    return isLessThanUnsigned(n1, n2) || (n1 == n2);
  }

  /**
   * Registers a slave. Drop registration request if numSlaves already
   * registered. Note that a slave re-registers under the same slaveID when
   * it comes back online.
   *
   * @param slave the slaveInfo to be registered
   *
   * @throws kvstore.KVException ERROR_SLAVE_QUOTA_ALREADY_MET if reached slave quota
   *                             and cannot register a new slave
   */
  public void registerSlave(TPCSlaveInfo slave) throws KVException {   
    synchronized (slaveMap) {
      long slaveID = slave.getSlaveID();
      if (slaveMap.containsKey(slaveID)) {
        TPCSlaveInfo slaveInfo = slaveMap.get(slaveID);
        slaveInfo.hostname = slave.getHostname();
        slaveInfo.port = slave.getPort();
      } else if (numSlaves > slaveMap.size()) {
        slaveMap.put(slaveID, slave);
        if (numSlaves == slaveMap.size()) {
          slaveMap.notifyAll();
        }
      } else {
        throw new KVException(ERROR_SLAVE_QUOTA_ALREADY_MET);
      }
    }   
  }

  /**
   * Find primary replica for a given key.
   *
   * @param key String to map to a slave server replica
   *
   * @return SlaveInfo of first replica
   */
  public TPCSlaveInfo findFirstReplica(String key) {  
    long slaveID = hashTo64bit(key);
    synchronized (slaveMap) {
      Map.Entry entry = slaveMap.ceilingEntry(slaveID);
      if (entry == null) {
        return slaveMap.firstEntry().getValue();
      }
      return (TPCSlaveInfo) entry.getValue();
    }   
  }

  /**
   * Find the successor of firstReplica.
   *
   * @param firstReplica SlaveInfo of primary replica
   *
   * @return SlaveInfo of successor replica
   */
  public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {   
    long slaveID = firstReplica.slaveID;
    synchronized (slaveMap) {
      Map.Entry entry = slaveMap.higherEntry(slaveID);
      if (entry == null) {
        return slaveMap.firstEntry().getValue();
      }
      return (TPCSlaveInfo) entry.getValue();
    }  
  }

  /**
   * @return The number of slaves currently registered.
   */
  public int getNumRegisteredSlaves() {   
    synchronized (slaveMap) {
      return slaveMap.size();
    } 
  }
 
  /**
   * For testing only.
   *
   * @param key
   * @param value
   */
  public void putIntoMasterCache(String key, String value) {
    masterCache.put(key, value);
  }
  

  /**
   * (For testing only) Attempt to get a registered slave's info by ID.
   *
   * @param slaveId
   *
   * @return The requested TPCSlaveInfo if present, otherwise null.
   */
  public TPCSlaveInfo getSlave(long slaveId) {
  
    synchronized (slaveMap) {
      return slaveMap.get(slaveId);
    }   
  }

  
  /**
   * Performs phase1 for the given slave replica
   *
   * @param slave
   * @param request
   *
   * @return vote
   */
  private String phase1(TPCSlaveInfo slave, KVMessage request) throws KVException {
    Socket slaveSocket = null;
    try {
      slaveSocket = slave.connectHost(TIMEOUT);
      request.sendMessage(slaveSocket);
      KVMessage voteMessage = new KVMessage(slaveSocket, TIMEOUT);
      String vote = voteMessage.getMsgType();
      if (vote.equals(ABORT)) {
        throw new KVException(voteMessage.getMessage());
      }
      return vote;
    } finally {
      slave.closeHost(slaveSocket);
    }
  }

  /**
   * Performs phase2 for the given slaveID replica
   *
   * @param slave
   * @param request
   *
   */
  private void phase2(Long slaveID, KVMessage decision) throws KVException {
    do {
      TPCSlaveInfo slave = null;
      Socket slaveSocket = null;
      try {
        slave = slaveMap.get(slaveID);
        slaveSocket = slave.connectHost(TIMEOUT);
        decision.sendMessage(slaveSocket);      
        String msgType = new KVMessage(slaveSocket, TIMEOUT).getMsgType();
        if (!msgType.isEmpty() && msgType.equals(ACK)) {          
          return;
        } else {       
          break;
        }
      } catch (KVException e) {
        //System.out.println("MASTER Phase2     " + e);
      } finally {
        slave.closeHost(slaveSocket);
      }
    } while (true);
    throw new KVException(ERROR_INVALID_FORMAT);
  }

  /**
   * Perform 2PC operations from the master node perspective. This method
   * contains the bulk of the two-phase commit logic. It performs phase 1
   * and phase 2 with appropriate timeouts and retries.
   *
   * See the spec for details on the expected behavior.
   *
   * @param request  KVMessage corresponding to the transaction for this TPC request
   * @param isPutReq boolean to distinguish put and del requests
   *
   * @throws KVException if the operation cannot be carried out for any reason
   */
  public synchronized void handleTPCRequest(KVMessage request, boolean isPutReq)
          throws KVException {
    waitForExpectedSlaves();

    // Phase 1
    String key = request.getKey();
    KVException operationFailed = null;
    boolean commit = false;
    try {

      masterCache.getLock(key).lock();

      TPCSlaveInfo primarySlave = findFirstReplica(key);
      TPCSlaveInfo secondarySlave = findSuccessor(primarySlave);

      String primaryResponse = "";
      String secondaryResponse = "";
      try {
        primaryResponse = phase1(primarySlave, request);
      } catch (KVException e) {
       // System.out.println("Primary PHASE 1 FAILURE   " + e);
        operationFailed = e;
      }
      try {
        secondaryResponse = phase1(secondarySlave, request);
      } catch (KVException e) {
       // System.out.println("Secondary PHASE 1 FAILURE   " + e);
        operationFailed = e;
      }

      if (primaryResponse.equals(READY) && secondaryResponse.equals(READY)) {
        commit = true;
      }

      // Phase 2
      KVMessage decision;
      if (commit) {
        decision = new KVMessage(COMMIT);
        // Update masterCache
        if (isPutReq == true) {
          masterCache.put(key, request.getValue());
        } else {
          masterCache.del(key);
        }
      } else {
        decision = new KVMessage(ABORT);
      }
      // If will receive anything other than ACK
      // then propagate KVException to client      
      phase2(primarySlave.getSlaveID(), decision);
      phase2(secondarySlave.getSlaveID(), decision);

    } finally {
      masterCache.getLock(key).unlock();
      if (operationFailed != null) {
        throw operationFailed;
      }
    }
  }

  /**
   * Perform GET operation in the following manner:
   * - Try to GET from cache, return immediately if found
   * - Try to GET from first/primary replica
   * - If primary succeeded, return value
   * - If primary failed, try to GET from the other replica
   * - If secondary succeeded, return value
   * - If secondary failed, return KVExceptions from both replicas
   *
   * @param msg KVMessage containing key to get
   *
   * @return value corresponding to the Key
   *
   * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
   *                     the value from either slave for any reason
   */
  public String handleGet(KVMessage msg) throws KVException {
    waitForExpectedSlaves();

    String key = msg.getKey();
    try {
      masterCache.getLock(key).lock();

      String cacheValue = masterCache.get(key);
      if (cacheValue != null) {
        return cacheValue;
      }

      TPCSlaveInfo primary = findFirstReplica(key);
      TPCSlaveInfo secondary = findSuccessor(primary);
      TPCSlaveInfo[] slaves = {primary, secondary};
      for (TPCSlaveInfo slave : slaves) {
        Socket socket = null;
        try {
          socket = slave.connectHost(TIMEOUT);
          msg.sendMessage(socket);
          KVMessage response = new KVMessage(socket, TIMEOUT);
          String responseType = response.getMsgType();
          String returnedMessage = response.getMessage();
          String returnedValue = response.getValue();
          if (responseType != null && responseType.equals(RESP)
              && returnedMessage == null && returnedValue != null) {
            // Found a match
            // Updating masterCache
            masterCache.put(key, returnedValue);
            return returnedValue;
          }
        } catch (KVException e) {
          System.out.println("Get attempt ended with exception: " + e);
        } finally {
          slave.closeHost(socket);
        }
      }
    } finally {
      masterCache.getLock(key).unlock();
    }
    throw new KVException(ERROR_NO_SUCH_KEY);
  }

  /**
   * Block the requestor until all the slaves have been registered
   */
  private void waitForExpectedSlaves() {
    synchronized (slaveMap) {
      while (numSlaves > slaveMap.size()) {
        try {
          slaveMap.wait();
        } catch (InterruptedException e) {
          System.out.println(e);

        }
      }
    }
  }

  /**
   * Serves as comparison function for the slaveMap
   */
  private class keyComparator implements Comparator<Long> {

    @Override
    public int compare(Long key1, Long key2) {
      if (isLessThanUnsigned(key1, key2)) {
        return -1;
      } else if (isLessThanEqualUnsigned(key1, key2)) {
        return 0;
      } else {
        return 1;
      }
    }
  }

}
