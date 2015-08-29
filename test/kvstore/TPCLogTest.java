package kvstore;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static kvstore.KVConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author mike
 */
public class TPCLogTest {

  private static final String LOG_PATH = "TPCMasterHandlerTest.log";
  private KVServer server;
  private TPCLog log;
  KVMessage m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11;

  @Before
  public void setUp() throws Exception {

    server = new KVServer(10, 10);
    log = new TPCLog(LOG_PATH, server);

    m1 = new KVMessage(PUT_REQ);
    m1.setKey("A");
    m1.setValue("Mike");
    m2 = new KVMessage(PUT_REQ);
    m2.setKey("B");
    m2.setValue("Peep");
    m3 = new KVMessage(DEL_REQ);
    m3.setKey("A");
    m4 = new KVMessage(DEL_REQ);
    m4.setKey("B");
    m5 = new KVMessage(GET_REQ);
    m5.setKey("A");
    m6 = new KVMessage(COMMIT);
    m7 = new KVMessage(ABORT);
    m8 = new KVMessage(PUT_REQ);
    m8.setKey("C");
    m8.setValue("Frank");
    m9 = new KVMessage(PUT_REQ);
    m9.setKey("D");
    m9.setValue("Dan");
    m10 = new KVMessage(DEL_REQ);
    m10.setKey("D");
    m11 = new KVMessage(DEL_REQ);
    m11.setKey("C");
  }

  @After
  public void tearDown() {
    server = null;
    log = null;
  }

  @Test
  public void testRebuildCommittedPuts() {
    log.appendAndFlush(m1);
    log.appendAndFlush(m6);
    log.appendAndFlush(m2);
    log.appendAndFlush(m6);
    log.appendAndFlush(m8);
    log.appendAndFlush(m6);
    log.appendAndFlush(m9);
    log.appendAndFlush(m6);

    try {
      log.rebuildServer();

      assertEquals(server.get("A"), "Mike");
      assertEquals(server.get("B"), "Peep");
      assertEquals(server.get("C"), "Frank");
      assertEquals(server.get("D"), "Dan");

    } catch (KVException ex) {
      System.out.println("1 " + ex);
    }
  }

 
  @Test
  public void testRebuildCommittedPutsDeletes() {
    log.appendAndFlush(m1);
    log.appendAndFlush(m6);
    log.appendAndFlush(m2);
    log.appendAndFlush(m6);
    log.appendAndFlush(m8);
    log.appendAndFlush(m6);
    log.appendAndFlush(m9);
    log.appendAndFlush(m6);

    log.appendAndFlush(m3);
    log.appendAndFlush(m6);
    log.appendAndFlush(m4);
    log.appendAndFlush(m6);
    log.appendAndFlush(m10);
    log.appendAndFlush(m6);
    log.appendAndFlush(m11);
    log.appendAndFlush(m6);

    try {
      log.rebuildServer();

      assertTrue(!server.hasKey("A"));
      assertTrue(!server.hasKey("B"));
      assertTrue(!server.hasKey("C"));
      assertTrue(!server.hasKey("D"));

    } catch (KVException ex) {
      System.out.println("3 " + ex);
    }
  }

  @Test
  public void testGetAbortMessages() {
    log.appendAndFlush(m5);
    log.appendAndFlush(m6);
    log.appendAndFlush(m2);
    log.appendAndFlush(m7);

    try {
      log.rebuildServer();

      assertTrue(!server.hasKey("A"));
      assertTrue(!server.hasKey("B"));

    } catch (KVException ex) {
      System.out.println("4 " + ex);
    }
  }

  @Test
  public void testImproperlyCommittedMessages() {
    log.appendAndFlush(m1);
    log.appendAndFlush(m6);
    log.appendAndFlush(m2);
    log.appendAndFlush(m3);
    log.appendAndFlush(m6);

    try {
      log.rebuildServer();

      assertTrue(!server.hasKey("A"));
      assertTrue(!server.hasKey("B"));

    } catch (KVException ex) {
      System.out.println("5 " + ex);
    }
  }
}
