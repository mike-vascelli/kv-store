package kvstore;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import kvstore.xml.KVCacheEntry;
import kvstore.xml.KVCacheType;
import kvstore.xml.KVSetType;
import kvstore.xml.ObjectFactory;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is dropped based on
 * the eviction policy.
 */
public class KVCache implements KeyValueInterface {

  final private KVCacheType cache;
  final private int numSets;
  final private int maxElemsPerSet;
  final private List<Lock> setLockList;

  /**
   * Constructs a second-chance-replacement cache.
   *
   * @param numSets        the number of sets this cache will have
   * @param maxElemsPerSet the size of each set
   */
  @SuppressWarnings("unchecked")
  public KVCache(int numSets, int maxElemsPerSet) {
    this.numSets = numSets;
    this.maxElemsPerSet = maxElemsPerSet;
    cache = new KVCacheType();
    setLockList = new ArrayList<>();
    for (int i = 0; i < numSets; i++) {
      KVSetType set = new KVSetType();
      set.setId(Integer.toString(i));
      cache.getSet().add(set);
      setLockList.add(new ReentrantLock());
    }
  }

  /**
   * Retrieves an entry from the cache.
   * Assumes access to the corresponding set has already been locked by the
   * caller of this method.
   *
   * @param key the key whose associated value is to be returned.
   *
   * @return the value associated to this key or null if no value is
   *         associated with this key in the cache
   */
  @Override
  public String get(String key) {
    int set_id = Math.abs(key.hashCode()) % numSets;
    for (KVCacheEntry entry : cache.getSet().get(set_id).getCacheEntry()) {
      if (entry.getKey().equals(key)) {
        entry.setIsReferenced("True");
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * Adds an entry to this cache.
   * If an entry with the specified key already exists in the cache, it is
   * replaced by the new entry. When an entry is replaced, its reference bit
   * will be set to True. If the set is full, an entry is removed from
   * the cache based on the eviction policy. If the set is not full, the entry
   * will be inserted behind all existing entries. For this policy, we suggest
   * using a LinkedList over an array to keep track of entries in a set since
   * deleting an entry in an array will leave a gap in the array, likely not
   * at the end. More details and explanations in the spec. Assumes access to
   * the corresponding set has already been locked by the caller of this
   * method.
   *
   * @param key   the key with which the specified value is to be associated
   * @param value a value to be associated with the specified key
   */
  @Override
  public void put(String key, String value) {
    int set_id = Math.abs(key.hashCode()) % numSets;
    List<KVCacheEntry> entryList = cache.getSet().get(set_id).getCacheEntry();
    for (KVCacheEntry entry : entryList) {
      if (entry.getKey().equals(key)) {
        entry.setValue(value);
        entry.setIsReferenced("True");
        return;
      }
    }
    if (getCacheSetSize(set_id) >= maxElemsPerSet) {  // Set is full. Must use second chance
      do {
        KVCacheEntry firstEntry = entryList.get(0);
        if (firstEntry.getIsReferenced().equals("False")) { // Evict
          firstEntry.setKey(key);
          firstEntry.setValue(value);
          return;
        } else { // Set to unreferenced, remove, and insert at end
          firstEntry.setIsReferenced("False");
          entryList.add(entryList.remove(0));
        }
      } while (true);
    } else { // Set has space. Just insert as last
      KVCacheEntry entry = new KVCacheEntry();
      entryList.add(entry);
      entry.setKey(key);
      entry.setValue(value);
      entry.setIsReferenced("False");
    }
  }

  /**
   * Removes an entry from this cache.
   * Assumes access to the corresponding set has already been locked by the
   * caller of this method. Does nothing if called on a key not in the cache.
   *
   * @param key key with which the specified value is to be associated
   */
  @Override
  public void del(String key) {
    int set_id = Math.abs(key.hashCode()) % numSets;
    List<KVCacheEntry> entryList = cache.getSet().get(set_id).getCacheEntry();
    for (KVCacheEntry entry : entryList) {
      if (entry.getKey().equals(key)) {
        entryList.remove(entry);
        return;
      }
    }
  }

  /**
   * Get a lock for the set corresponding to a given key.
   * The lock should be used by the caller of the get/put/del methods
   * so that different sets can be #{modified|changed} in parallel.
   *
   * @param key key to determine the lock to return
   *
   * @return lock for the set that contains the key
   */
  public Lock getLock(String key) {
    int set_id = Math.abs(key.hashCode()) % numSets;
    return setLockList.get(set_id);

  }

  /**
   * Get the size of a given set in the cache.
   *
   * @param cacheSet Which set.
   *
   * @return Size of the cache set.
   */
  int getCacheSetSize(int cacheSet) {
    return cache.getSet().get(cacheSet).getCacheEntry().size();
  }

  private void marshalTo(OutputStream os) throws JAXBException {
    JAXBContext context = JAXBContext.newInstance(KVCacheType.class);
    Marshaller marshaller = context.createMarshaller();
    marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, false);
    marshaller.marshal(getXMLRoot(), os);
  }

  private JAXBElement<KVCacheType> getXMLRoot() throws JAXBException {
    ObjectFactory factory = new ObjectFactory();
    KVCacheType xmlCache = factory.createKVCacheType();
    for (KVSetType set : cache.getSet()) {
      KVSetType xmlSet = factory.createKVSetType();
      xmlCache.getSet().add(xmlSet);
      xmlSet.setId(set.getId());
      for (KVCacheEntry entry : set.getCacheEntry()) {
        KVCacheEntry xmlEntry = factory.createKVCacheEntry();
        xmlSet.getCacheEntry().add(xmlEntry);
        xmlEntry.setIsReferenced(entry.getIsReferenced());
        xmlEntry.setKey(entry.getKey());
        xmlEntry.setValue(entry.getValue());
      }
    }
    return factory.createKVCache(xmlCache);
  }

  /**
   * Serialize this store to XML. See spec for details on output format.
   */
  public String toXML() {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      marshalTo(os);
    } catch (JAXBException e) {
      //e.printStackTrace();
    }
    return os.toString();
  }

  @Override
  public String toString() {
    return this.toXML();
  }

}
