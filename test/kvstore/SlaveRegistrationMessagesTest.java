package kvstore;

import static autograder.TestUtils.kTimeoutQuick;
import static kvstore.KVConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.net.Socket;

import javax.xml.parsers.*;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.w3c.dom.*;
import org.xml.sax.*;

import autograder.AGCategories.AGTestDetails;
import autograder.AGCategories.AG_PROJ3_CODE;

/**
 *
 * @author mike
 */
public class SlaveRegistrationMessagesTest {

  private static File tempFile;

  private Socket sock;
  private InputStream stream;
  private PrintStream stdErr;

  @BeforeClass
  public static void setupTempFile() throws IOException {
    tempFile = File.createTempFile("TestKVMessage-", ".txt");
    tempFile.deleteOnExit();
  }
  

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(
          desc = "Must be able to parse register request successfully")
  public void successfullyParsesDelReq() throws KVException {
    sock = Utils.setupReadFromFile("registerreq.txt");
    KVMessage kvm = new KVMessage(sock);
    assertNotNull(kvm);
    assertEquals(REGISTER, kvm.getMsgType());
    assertNotNull(kvm.getMessage());
    assertNull(kvm.getKey());
    assertNull(kvm.getValue());
  }

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(
          desc = "Must be able to parse register response successfully")
  public void successfullyParsesDelResp() throws KVException {
    sock = Utils.setupReadFromFile("registerresp.txt");
    KVMessage kvm = new KVMessage(sock);
    assertNotNull(kvm);
    assertEquals(RESP, kvm.getMsgType());
    assertTrue("Successfully registered 11111111@255.255.255.255:5000".equals(kvm.getMessage()));
    assertNull(kvm.getKey());
    assertNull(kvm.getValue());
  } 

  
}
