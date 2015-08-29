package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Data structure to maintain information about SlaveServers
 */
public class TPCSlaveInfo {

  public long slaveID;
  public String hostname;
  public int port;

  /**
   * Construct a TPCSlaveInfo to represent a slave server.
   *
   * @param info as "SlaveServerID@Hostname:Port"
   *
   * @throws KVException ERROR_INVALID_FORMAT if info string is invalid
   */
  public TPCSlaveInfo(String info) throws KVException {       
    final String REGEX = "(-{0,1}\\d+)@(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)";
    Pattern pattern = Pattern.compile(REGEX);
    Matcher matcher = pattern.matcher(info);
    if (matcher.matches()) {
      try {
        slaveID = Long.parseLong(matcher.group(1));
        port = Integer.parseInt(matcher.group(3));
      } catch (NumberFormatException e) {
        throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
      }
      hostname = matcher.group(2);
    }
    if (!validIPAddress(hostname)) {
      throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
    }  
  }

  
  /**
   * Validates the hostname IP address
   *
   * @param hostname
   *
   * @return
   */
  private boolean validIPAddress(String hostname) {
    if (hostname == null) {
      return false;
    }
    String[] octets = hostname.split("\\.");
    for (String octet : octets) {
      try {
        int value = Integer.parseInt(octet);
        if (value < 0 || value > 255) {
          return false;
        }
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return true;
  }
 

  public long getSlaveID() {
    return slaveID;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  /**
   * Create and connect a socket within a certain timeout.
   *
   * @param timeout
   * @return Socket object connected to SlaveServer, with timeout set
   *
   * @throws KVException ERROR_SOCKET_TIMEOUT, ERROR_COULD_NOT_CREATE_SOCKET,
   *                     or ERROR_COULD_NOT_CONNECT
   */
  public Socket connectHost(int timeout) throws KVException {   
    try {
      Socket sock = new Socket();
      sock.connect(new InetSocketAddress(hostname, port), timeout);
      return sock;
    } catch (SocketTimeoutException e) {
      throw new KVException(ERROR_SOCKET_TIMEOUT);
    } catch (IOException e) {
      throw new KVException(ERROR_COULD_NOT_CONNECT);
    } catch (IllegalArgumentException e) {
      throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
    } 
    
  }

  /**
   * Closes a socket.
   * Best effort, ignores error since the response has already been received.
   *
   * @param sock Socket to be closed
   */
  public void closeHost(Socket sock) {
    try {     
       if (sock != null) {
        sock.close();
      }
    } catch (IOException e) {
      System.out.println(e);
    }
  }
  
}
