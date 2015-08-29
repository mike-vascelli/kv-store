package kvstore;

import static autograder.TestUtils.*;
import static kvstore.KVConstants.*;
import static kvstore.Utils.assertKVExceptionEquals;
import static kvstore.Utils.*;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Random;

import kvstore.Utils.ErrorLogger;
import kvstore.Utils.RandomString;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import autograder.AGCategories.AGTestDetails;
import autograder.AGCategories.AG_PROJ3_CODE;
import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EndToEndTest extends EndToEndTemplate {

  @Test
  public void testSome() {
    
    try {
      client.put("peppe1", "PEPPE1");
      client.put("peppe2", "PEPPE2");
      client.put("peppe3", "PEPPE3");
      client.put("peppe4", "PEPPE4");
      client.put("peppe5", "PEPPE5");
      client.put("peppe6", "PEPPE6");
      client.put("peppe7", "PEPPE7");

      assertEquals("PEPPE1", client.get("peppe1"));
      assertEquals("PEPPE2", client.get("peppe2"));
      assertEquals("PEPPE3", client.get("peppe3"));
      assertEquals("PEPPE4", client.get("peppe4"));
      assertEquals("PEPPE5", client.get("peppe5"));
      assertEquals("PEPPE6", client.get("peppe6"));
      assertEquals("PEPPE7", client.get("peppe7"));

      client.del("peppe6");
      assertTrue("Success".equals(KVClient.retrieveServerResponse(KVConstants.DEL_REQ)));

      try {
        client.get("peppe6");
        assertTrue("peppe6 PEPPE6".equals(KVClient.retrieveServerResponse(KVConstants.GET_REQ)));
        fail("Get used a non-existent key");
      } catch (KVException e) {
        String errMsg = e.getKVMessage().getMessage();
        assertTrue(errMsg.equals(ERROR_NO_SUCH_KEY));
      }

      client.put("peppe6", "PEPPE6");
      assertTrue("Success".equals(KVClient.retrieveServerResponse(KVConstants.PUT_REQ)));
      assertEquals("PEPPE6", client.get("peppe6"));

    } catch (KVException ex) {
      System.out.println("EXCEPTION IS: " + ex);
    }

  }

  

}
