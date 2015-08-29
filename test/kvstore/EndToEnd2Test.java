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
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.junit.After;
import org.junit.Before;

public class EndToEnd2Test {

  ServerRunner registrationHandlerRunner = null;
  ServerRunner clientHandlerRunner = null;
  ServerRunner slaveRunner = null;

  SocketServer masterClientSocketServer;
  SocketServer masterSlaveSocketServer;
  
  TPCMaster tpcMaster;

  KVClient client;

  String hostname;
  
  ServerRunner[] slaves;

  KVServer[] slaveKvs;
  
  final long SLAVE1 = 1L;
  final long SLAVE2 = 2L;

  @Before
  public void setUp() throws Exception {
    hostname = InetAddress.getLocalHost().getHostAddress();

    startMaster();
    
    slaves = new ServerRunner[2];
    slaveKvs = new KVServer[2];

    startSlave(SLAVE1, 0);
    startSlave(SLAVE2, 1);

    client = new KVClient(hostname, 8080);
  }

  @After
  public void tearDown() throws InterruptedException {
    registrationHandlerRunner.stop();
    clientHandlerRunner.stop();
    slaveRunner.stop();    

    slaves = null;
    slaveKvs = null;
    client = null;
    registrationHandlerRunner = null;
    slaveRunner = null;
    
    clientHandlerRunner = null;
  }

  private void startMaster() {
    try {
      hostname = InetAddress.getLocalHost().getHostAddress();
      tpcMaster = new TPCMaster(2, new KVCache(1, 4));

      masterSlaveSocketServer = new SocketServer(hostname, 9090);
      NetworkHandler slaveHandler = new TPCRegistrationHandler(tpcMaster);
      masterSlaveSocketServer.addHandler(slaveHandler);
      registrationHandlerRunner
      = new ServerRunner(masterSlaveSocketServer, "slaveListener");
      registrationHandlerRunner.start();

      masterClientSocketServer = new SocketServer(hostname, 8080);
      NetworkHandler clientHandler = new TPCClientHandler(tpcMaster);
      masterClientSocketServer.addHandler(clientHandler);
      clientHandlerRunner
      = new ServerRunner(masterClientSocketServer, "clientListener");
      clientHandlerRunner.start();
      Thread.sleep(100);
    } catch (UnknownHostException | InterruptedException e) {
      System.out.println(e);
    }
  }

  private void startSlave(long id, int index) {
    try {     
    SocketServer slaveSocketServer = new SocketServer(InetAddress.getLocalHost().getHostAddress());    
    slaveKvs[index] = new KVServer(100, 10);
    File temp = File.createTempFile(Long.toString(id) + "calbandgreat", ".txt");
    temp.deleteOnExit();
    String logPath = temp.getPath(); //"bin/log." + slaveID + "@" + ss.getHostname();
    TPCLog log = new TPCLog(logPath, slaveKvs[index]);
    TPCMasterHandler handler = new TPCMasterHandler(id, slaveKvs[index], log);
    slaveSocketServer.addHandler(handler);
    slaveRunner = new ServerRunner(slaveSocketServer, Long.toString(id));
    slaves[index] = slaveRunner;
    slaveRunner.start();
    handler.registerWithMaster(InetAddress.getLocalHost().getHostAddress(), slaveSocketServer);
    Thread.sleep(100);
    } catch (IOException | KVException | InterruptedException e) {
      System.out.println(e);
    }    
  }

  @Test
  public void testFailedTPCGet() {
    try {
      client.get("peppe");
    } catch (KVException e) {
      assertEquals(e.getMessage(), ERROR_NO_SUCH_KEY);
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }

  @Test
  public void testSuccessfulMasterCacheTPCGet() {
    try {      
      // Slaves have no data
      tpcMaster.putIntoMasterCache("peppe", "pippo");
      String value = client.get("peppe");
      assertEquals(value, "pippo");
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }
  
  @Test
  public void testSuccessfulPrimarySlaveTPCGet() {
    try {      
      slaveKvs[0].putIntoCache("peppe", "pippo");
      String slaveValue = client.get("peppe");
      assertEquals(slaveValue, "pippo");
     
      String cacheValue =  tpcMaster.masterCache.get("peppe");
      assertEquals(cacheValue, "pippo");
    } catch (Exception e) {
      fail("Unexpected Exception");
    }
  }
  
  @Test
  public void testSuccessfulSecondarySlaveTPCGet() {
    try {      
      slaveKvs[1].putIntoCache("peppe", "pippo");      
      String slaveValue = client.get("peppe");
      assertEquals(slaveValue, "pippo");
      
      String cacheValue =  tpcMaster.masterCache.get("peppe");
      assertEquals(cacheValue, "pippo");
    } catch (Exception e) {
      System.out.println(e);
      fail("Unexpected Exception");
    }
  }

}
