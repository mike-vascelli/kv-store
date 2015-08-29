package kvstore;

import static autograder.TestUtils.kTimeoutQuick;
import static kvstore.KVConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.assertTrue;
import org.powermock.core.classloader.annotations.PowerMockIgnore;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TPCMasterHandler.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.internal.impl.dv.*"})
public class TPCMasterHandlerTest {

  private static final String LOG_PATH = "TPCMasterHandlerTest.log";

  private KVServer server;
  private TPCMasterHandler masterHandler;
  private Socket sock1;
  private Socket sock2;
  private Socket sock3;
  ServerRunner serverRunner = null;

  @Before
  public void setupTPCMasterHandler() throws Exception {
    server = new KVServer(10, 10);
    TPCLog log = new TPCLog(LOG_PATH, server);
    Utils.setupMockThreadPool();
    masterHandler = new TPCMasterHandler(1L, server, log);
  }

  @After
  public void tearDown() {
    server = null;
    masterHandler = null;
    sock1 = null;
    sock2 = null;
    sock3 = null;
    File log = new File(LOG_PATH);
    try {
      if (serverRunner != null) {
        serverRunner.stop();
      }
    } catch (InterruptedException ex) {
      System.out.println(ex);
    }

    if (log.exists() && !log.delete()) { // true iff delete failed.
      System.err.printf("deleting log-file at %s failed.\n", log.getAbsolutePath());
    }
  }

  @Test(timeout = kTimeoutQuick)
  @Category(AG_PROJ4_CODE.class)
  @AGTestDetails(points = 1,
          desc = "Test if they can fail to get one value using Handler")
  public void testFailureGet() throws KVException {
    setupSocketSuccess();
    InputStream getreqFile = getClass().getClassLoader().getResourceAsStream("getreq.txt");
    ByteArrayOutputStream tempOut = new ByteArrayOutputStream();
    try {
      doNothing().when(sock1).setSoTimeout(anyInt());
      when(sock1.getInputStream()).thenReturn(getreqFile);
      when(sock1.getOutputStream()).thenReturn(tempOut);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    masterHandler.handle(sock1);

    try {
      doNothing().when(sock3).setSoTimeout(anyInt());
      when(sock3.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut.toByteArray()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    KVMessage check = new KVMessage(sock3);
    assertEquals(RESP, check.getMsgType());
    assertEquals(ERROR_NO_SUCH_KEY, check.getMessage());
  }

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Check if slave can register with master")
  
  public void testRegisterWithMaster() throws Exception {
    Socket socket = null;
    String hostname = InetAddress.getLocalHost().getHostAddress();

    try {
      SocketServer ss = new SocketServer(hostname, 9090);
      TPCMaster master = new TPCMaster(3, new KVCache(4, 4));
      ss.addHandler(new TPCRegistrationHandler(master));
      serverRunner = new ServerRunner(ss, "server1");
      serverRunner.start();
      Thread.sleep(100);

      masterHandler.registerWithMaster(hostname, ss);

    } catch (KVException kve) {
      System.out.println(kve);
      kve.printStackTrace();
      fail("unexpected exception");
    } catch (InterruptedException e) {
    }
  }

  
  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Check if slave is not allowed to register")
  public void testFailedRegisterWithMaster() throws Exception {
    Socket socket = null;
    String hostname = InetAddress.getLocalHost().getHostAddress();
    try {
      SocketServer ss = new SocketServer(hostname, 9090);
      TPCMaster master = new TPCMaster(3, new KVCache(4, 4));
      TPCSlaveInfo slave1 = new TPCSlaveInfo("11@255.255.255.255:8000");
      TPCSlaveInfo slave2 = new TPCSlaveInfo("111@255.255.255.255:8000");
      TPCSlaveInfo slave3 = new TPCSlaveInfo("1111@255.255.255.255:8000");
      TPCSlaveInfo slave4 = new TPCSlaveInfo("11111@255.255.255.255:8000");
      master.registerSlave(slave1);
      master.registerSlave(slave2);
      master.registerSlave(slave3);
      master.registerSlave(slave4);

      ss.addHandler(new TPCRegistrationHandler(master));
      serverRunner = new ServerRunner(ss, "server");
      serverRunner.start();
      Thread.sleep(100);

      masterHandler.registerWithMaster(hostname, ss);
    } catch (KVException kve) {
      //System.out.println(kve);
      assertTrue(kve.getMessage().equals(ERROR_SLAVE_QUOTA_ALREADY_MET));
    } catch (Exception e) {
    } finally {
      if (socket != null) {
        socket.close();

      }
    }
  }

  /* begin helper methods. */
  private void setupSocketSuccess() {
    sock1 = mock(Socket.class);
    sock2 = mock(Socket.class);
    sock3 = mock(Socket.class);
  }

}
