package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;

import java.util.Random;


import org.junit.Test;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class EndToEnd3Test extends TPCEndToEndTemplate {

  @Test
  public void testSuccessfulGet() {
    try {
      client.put("peppe", "pippo");
      String value = client.get("peppe");
      assertEquals(value, "pippo");
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testSuccessfulTPCput() {
    try {
      client.put("peppe", "pippo");
      assertEquals(client.get("peppe"), "pippo");
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testEmptyKeyTPCput() {
    try {
      client.put("", "pippo");
      fail("Should fail");
    } catch (KVException e) {
      assertEquals(e.getMessage(), ERROR_INVALID_KEY);
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testEmptyValueTPCput() {
    try {
      client.put("pippo", "");
      fail("Should fail");
    } catch (KVException e) {
      assertEquals(e.getMessage(), ERROR_INVALID_VALUE);
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testSuccessfulTPCdel() {
    try {
      client.put("peppe", "pippo");
      client.del("peppe");
      client.get("peppe");
      fail("Should have thrown NO_KEY_ERROR");
    } catch (KVException e) {
      assertEquals(e.getKVMessage().getMessage(), ERROR_NO_SUCH_KEY);
    }
  }

  @Test
  public void testFailedGet() {
    try {
      client.get("peppe");
      fail("Should have failed");
    } catch (KVException e) {
      assertEquals(e.getMessage(), ERROR_NO_SUCH_KEY);
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testFailedTPCPutBigValue() {
    try {
      client.put("peppe", generateString(MAX_VAL_SIZE, "p"));
      fail("Should have failed");
    } catch (KVException e) {
      assertEquals(e.getMessage(), ERROR_OVERSIZED_VALUE);
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testFailedTPCPutBigKey() {
    try {
      client.put(generateString(MAX_KEY_SIZE, "p"), "pippo");
      fail("Should have failed");
    } catch (KVException e) {
      assertEquals(e.getMessage(), ERROR_OVERSIZED_KEY);
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testFailedTPCDel() {
    try {
      client.del("peppe");
      fail("Should have thrown NO_KEY_ERROR");
    } catch (KVException e) {
      assertEquals(e.getKVMessage().getMessage(), ERROR_NO_SUCH_KEY);
    }
  }

  @Test
  public void testDuplicateSuccessfulPut() {
    try {
      client.put("peppe", "super");
      client.put("peppe", "pippo");
      assertEquals(client.get("peppe"), "pippo");
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testStressSystem() {
    HashMap<String, String> storeVerifier = new HashMap<>();
    final int TOTAL_OPERATIONS = 100;
    String key;
    String value;
    String result;

    Random rand = new Random();
    int iteration = 0;
    do {
      int opcode = rand.nextInt(3);
      
      switch (opcode) {
        case 0: //  PUT
          key = generateString(rand.nextInt(5), "p");
          value = generateString(rand.nextInt(10000), "p");
          storeVerifier.put(key, value);
          try {
            client.put(key, value);
            assertEquals(value, client.get(key));
          } catch (Exception ex) {
            fail("Put should not fail");
          }
          break;
        case 1: // DEL
          key = generateString(rand.nextInt(5), "p");
          result = storeVerifier.remove(key);
          try {
            client.del(key);
            if (result == null) {
              fail("Del should have failed");
            } else {
              try {
                client.get(key);
              } catch (KVException e) {
                assertEquals(e.getMessage(), ERROR_NO_SUCH_KEY);
              }
            }
          } catch (KVException ex) {
            if (result == null) {
              assertEquals(ex.getMessage(), ERROR_NO_SUCH_KEY);
            } else {
              fail("Del should have succeeded");
            }
          }
          break;
        case 2: // GET
          key = generateString(rand.nextInt(5), "p");
          result = storeVerifier.get(key);
          try {
            client.get(key);
            if (result == null) {
              fail("Get should have failed");
            }
          } catch (KVException ex) {
            if (result == null) {
              assertEquals(ex.getMessage(), ERROR_NO_SUCH_KEY);
            } else {
              fail("Get should have succeeded");
            }
          }
          break;
        default: // ERROR CASE
          System.out.println("Wrong opcode");
          fail();
      }
    } while (++iteration < TOTAL_OPERATIONS);
  }

  private String generateString(final int MAX_LENGTH, String str) {
    String value = str;
    do {
      value += value;
    } while (value.length() < (MAX_LENGTH + 1));
    return value;
  }
  
  

}
