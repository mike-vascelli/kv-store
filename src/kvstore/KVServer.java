package kvstore;

import static kvstore.KVConstants.*;

/**
 * This class services all storage logic for an individual key-value server.
 * All KVServer request on keys from different sets must be parallel while
 * requests on keys from the same set should be serial. A write-through
 * policy should be followed when a put request is made.
 */
public class KVServer implements KeyValueInterface {

  private KVStore dataStore;
  private KVCache dataCache;  

  /**
   * Constructs a KVServer backed by a KVCache and KVStore.
   *
   * @param numSets        the number of sets in the data cache
   * @param maxElemsPerSet the size of each set in the data cache
   */
  public KVServer(int numSets, int maxElemsPerSet) {
    this.dataCache = new KVCache(numSets, maxElemsPerSet);
    this.dataStore = new KVStore();
  }

  /**
   * Performs put request on cache and store.
   *
   * @param key   String key
   * @param value String value
   *
   * @throws KVException if key or value is too long
   */
  @Override
  public void put(String key, String value) throws KVException {
    if (key.length() > MAX_KEY_SIZE) {
      throw new KVException(KVConstants.ERROR_OVERSIZED_KEY);
    }
    if (value.length() > MAX_VAL_SIZE) {
      throw new KVException(KVConstants.ERROR_OVERSIZED_VALUE);
    }
    try {
      dataCache.getLock(key).lock();
      dataCache.put(key, value);
      dataStore.put(key, value);
    } finally {
      dataCache.getLock(key).unlock();
    }
  }

  /**
   * Performs get request.
   * Checks cache first. Updates cache if not in cache but located in store.
   *
   * @param key String key
   *
   * @return String value associated with key
   *
   * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
   */
  @Override
  public String get(String key) throws KVException {
    try {
      dataCache.getLock(key).lock();
      String cacheValue = dataCache.get(key);
      if (cacheValue == null) {
        String storeValue = dataStore.get(key);
        dataCache.put(key, storeValue);
        return storeValue;
      }
      return cacheValue;
    } finally {
      dataCache.getLock(key).unlock();
    }
  }

  /**
   * Performs del request.
   *
   * @param key String key
   *
   * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
   */
  @Override
  public void del(String key) throws KVException {
    try {
      dataCache.getLock(key).lock();
      dataCache.del(key);
      dataStore.del(key);
    } finally {
      dataCache.getLock(key).unlock();
    }
  }

  /**
   * Check if the server has a given key. This is used for TPC operations
   * that need to check whether or not a transaction can be performed but
   * you don't want to modify the state of the cache by calling get(). You
   * are allowed to call dataStore.get() for this method.
   *
   * @param key key to check for membership in store
   * @return 
   */
  public boolean hasKey(String key) {
    try {
      dataStore.get(key);
    } catch (KVException e) {
      return false;
    }
    return true;
  }

  /** This method is purely for convenience and will not be tested. */
  @Override
  public String toString() {
    return dataStore.toString() + dataCache.toString();
  }
  

  /**
   * For testing only.
   * @param key
   * @param value
   */
  public void putIntoCache(String key, String value) {
    try {
      put(key, value);
    } catch (KVException ex) {
      System.out.println(ex);
    }
  }
 

}
