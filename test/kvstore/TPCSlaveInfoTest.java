package kvstore;

import static autograder.TestUtils.*;
import static kvstore.KVConstants.*;
import static kvstore.Utils.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.net.*;
import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import autograder.AGCategories.AGTestDetails;
import autograder.AGCategories.AG_PROJ3_CODE;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TPCSlaveInfo.class)
/**
 *
 * @author mike
 */
public class TPCSlaveInfoTest {

  private TPCSlaveInfo slave;
  private ServerRunner serverRunner;

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Slave should successfully utilize proper input")
  public void testValidSlaveData() throws Exception {
    try {
      slave = new TPCSlaveInfo("11111@255.255.255.255:8000");
      assertTrue(slave.getSlaveID() == 11111);
      assertTrue(slave.getHostname().equals("255.255.255.255"));
      assertTrue(slave.getPort() == 8000);
    } catch (Exception e) {
      fail("slave threw an unexpected exception");
    }
  }

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Slave should successfully parse negative slaveID's")
  public void testNegativeSlaveID() throws Exception {
    try {
      slave = new TPCSlaveInfo("-11111@255.255.255.255:8000");
      assertTrue(slave.getSlaveID() == -11111);
    } catch (Exception e) {
      fail("slave threw an unexpected exception");
    }
  }

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Slave should raise ERROR_INVALID_FORMAT on improper ID")
  public void testInvalidSlaveID() throws Exception {
    final int CHECKS_NUM = 6;
    int current = 0;
    try {
      try {
        slave = new TPCSlaveInfo("--111@ 255.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("1 11@255.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111 @255.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111asd@255.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111111111111111111111111111111111111111@255.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("- 111@255.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      assertTrue(current == CHECKS_NUM);
    } catch (Exception e) {
      fail("Unexpected exception." + e);
    }
  }

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Slave should raise ERROR_INVALID_FORMAT on improper host")
  public void testInvalidSlaveHost() throws Exception {
    final int CHECKS_NUM = 8;
    int current = 0;
    try {
      try {
        slave = new TPCSlaveInfo("111@ 255.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@255.  255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@255255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@a55.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@33255.255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@255...255.255.255:8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@255.255.255.255 :8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@localhost:8000");
      } catch (KVException e) {
        current++;
      }
      assertTrue(current == CHECKS_NUM);
    } catch (Exception e) {
      fail("Unexpected exception." + e);
    }
  }

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Slave should raise ERROR_INVALID_FORMAT on improper port")
  public void testInvalidSlavePort() throws Exception {
    final int CHECKS_NUM = 5;
    int current = 0;
    try {
      try {
        slave = new TPCSlaveInfo("111@ 255.255.255.255: 8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@255.255.255.255:8000 ");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@255.255.255.255:800 0");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@255.255.255.255:-8000");
      } catch (KVException e) {
        current++;
      }
      try {
        slave = new TPCSlaveInfo("111@255.255.255.255:a800");
      } catch (KVException e) {
        current++;
      }
      assertTrue(current == CHECKS_NUM);
    } catch (Exception e) {
      fail("Unexpected exception." + e);
    }
  }

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Slave should raise ERROR_COULD_NOT_CONNECT on UnknownHostException")
  public void testImpossibleConnection() throws Exception {
    try {
      slave = new TPCSlaveInfo("11111@255.255.255.255:8000");
      slave.connectHost(0);
      fail("slave did not throw a KVException!");
    } catch (KVException kve) {
      String errMsg = kve.getKVMessage().getMessage();
      assertTrue(errMsg.equals(ERROR_COULD_NOT_CONNECT)
                 || errMsg.equals(ERROR_COULD_NOT_CREATE_SOCKET));
    }
  }

  @Test(timeout = kTimeoutQuick)
  @AGTestDetails(points = 1,
          desc = "Slave should successfully connect")
  public void testValidNetworkingData() throws Exception {
    Socket socket = null;
    final String LOG_PATH = "TPCMasterHandlerTest.log";
    KVServer server = new KVServer(10, 10);
    TPCLog log =  new TPCLog(LOG_PATH, server);
    String hostname = InetAddress.getLocalHost().getHostAddress();
    try {
      SocketServer ss = new SocketServer(hostname, 8080);
      ss.addHandler(new TPCMasterHandler(1L, server, log));
      serverRunner = new ServerRunner(ss, "server");
      serverRunner.start();
      
      slave = new TPCSlaveInfo("11111@127.0.1.1:8080");
      socket = slave.connectHost(0);     
    } catch (KVException kve) {
      System.out.println(kve);
      fail("unexpected exception");
    } finally {
      if (socket != null) {
        socket.close();                     
      }
    }
  }  

}
