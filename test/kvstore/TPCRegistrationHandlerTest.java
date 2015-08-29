package kvstore;

import static autograder.TestUtils.kTimeoutQuick;
import static kvstore.KVConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import autograder.AGCategories.AGTestDetails;
import autograder.AGCategories.AG_PROJ4_CODE;
import java.util.logging.Level;
import java.util.logging.Logger;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TPCRegistrationHandler.class)
public class TPCRegistrationHandlerTest {

  private final static int MAX_SLAVES = 3;
  private final static String[] MESSAGE_FILES = {"registerreq.txt", "registerreq1.txt", "registerreq2.txt", "registerreq3.txt"};
  private final static String[] MESSAGE_RESPONSES = {"Successfully registered 11111111@255.255.255.255:5000",
                                                     "Successfully registered 1111111134@255.255.255.255:5000",
                                                     "Successfully registered 2222222222@255.255.255.255:5000",
                                                     "Warning: Could not register this slave, as the expected quota is already met."};
  private TPCMaster master;
  private TPCRegistrationHandler registrationHandler;
  private Socket sock1;
  private Socket sock3;

  @Before
  public void setupTPCRegistrationHandler() throws Exception {
    master = new TPCMaster(MAX_SLAVES, new KVCache(4, 4));
    Utils.setupMockThreadPool();
    registrationHandler = new TPCRegistrationHandler(master);
  }

  @After
  public void tearDown() {
    master = null;
    registrationHandler = null;
    sock1 = null;
    sock3 = null;
  }

  /**
   * This test will check the responses to 4 registration requests.
   * THe last one must fail because the master will have reached the 
   * MAX_SLAVES quota by then.
   *
   * @throws KVException
   */
  @Test(timeout = kTimeoutQuick)
  @Category(AG_PROJ4_CODE.class)
  @AGTestDetails(points = 1,
          desc = "Test registration process by analyzing the response messages")
  public void testRegistrationResponse() throws KVException {
    setupSocketSuccess();

    for (int i = 0; i < MAX_SLAVES + 1; i++) {

      InputStream getreqFile = getClass().getClassLoader().getResourceAsStream(MESSAGE_FILES[i]);
      ByteArrayOutputStream tempOut = new ByteArrayOutputStream();
      try {
        doNothing().when(sock1).setSoTimeout(anyInt());
        when(sock1.getInputStream()).thenReturn(getreqFile);
        when(sock1.getOutputStream()).thenReturn(tempOut);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      registrationHandler.handle(sock1);

      try {
        doNothing().when(sock3).setSoTimeout(anyInt());
        when(sock3.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut.toByteArray()));

      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      KVMessage check = new KVMessage(sock3);
      assertEquals(RESP, check.getMsgType());
      assertEquals(MESSAGE_RESPONSES[i], check.getMessage());
    }
  }

  /* begin helper methods. */
  private void setupSocketSuccess() {
    sock1 = mock(Socket.class);
    sock3 = mock(Socket.class);
  }

}
