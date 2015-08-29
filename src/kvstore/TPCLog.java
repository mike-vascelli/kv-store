package kvstore;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import static kvstore.KVConstants.ABORT;
import static kvstore.KVConstants.COMMIT;
import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.PUT_REQ;

public class TPCLog {

  private String logPath;
  private KVServer kvServer;
  private ArrayList<KVMessage> entries;

  /**
   * Constructs a TPCLog to log KVMessages from the master.
   *
   * @param logPath  path to location of log file for this server
   * @param kvServer reference to the KVServer of this slave
   * @throws kvstore.KVException
   */
  public TPCLog(String logPath, KVServer kvServer) throws KVException {
    this.logPath = logPath;
    this.kvServer = kvServer;
    this.entries = new ArrayList<>();
    rebuildServer();
  }

  /**
   * Add an entry to the log and flush the entire log to disk.
   * You do not have to efficiently append entries onto the log stored on disk.
   *
   * @param entry KVMessage to write to the log
   */
  public void appendAndFlush(KVMessage entry) {
    entries.add(entry);
    flushToDisk();
  }

  /**
   * Get last entry in the log.
   *
   * @return last entry put into the log
   */
  public KVMessage getLastEntry() {
    if (entries.size() > 0) {
      return entries.get(entries.size() - 1);
    }
    return null;
  }

  /**
   * Load log from persistent storage at logPath.
   */
  @SuppressWarnings("unchecked")
  public void loadFromDisk() {
    ObjectInputStream inputStream = null;

    try {
      inputStream = new ObjectInputStream(new FileInputStream(logPath));
      entries = (ArrayList<KVMessage>) inputStream.readObject();
    } catch (Exception e) {
    } finally {
      // if log did not exist, creating empty entries list
      if (entries == null) {
        entries = new ArrayList<KVMessage>();
      }

      try {
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Writes the log to persistent storage at logPath.
   */
  public void flushToDisk() {
    ObjectOutputStream outputStream = null;

    try {
      outputStream = new ObjectOutputStream(new FileOutputStream(logPath));
      outputStream.writeObject(entries);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (outputStream != null) {
          outputStream.flush();
          outputStream.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Load log and rebuild KVServer by iterating over log entries. You do not
   * need to restore the previous cache state (i.e. ignore GETS).
   *
   * @throws KVException if an error occurs in KVServer (though we expect none)
   */
  public void rebuildServer() throws KVException {
    loadFromDisk();
       
    KVMessage committedRequest = null;
    for (KVMessage entry : entries) {
      String entryType = entry.getMsgType();
      if (entryType.equals(DEL_REQ) || entryType.equals(PUT_REQ)) {
        committedRequest = entry;
      } else if (entryType.equals(ABORT)) {
        committedRequest = null;
      } else if (entryType.equals(COMMIT) && committedRequest != null) {
        String key = committedRequest.getKey();
        String type = committedRequest.getMsgType();
        if (type.equals(PUT_REQ)) {
          kvServer.put(key, committedRequest.getValue());
        } else if (type.equals(DEL_REQ)) {
          kvServer.del(key);
        }
        committedRequest = null;
      }
    }

  }
       
}
