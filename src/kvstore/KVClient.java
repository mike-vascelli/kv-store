package kvstore;

import java.io.IOException;
import java.net.Socket;
import static kvstore.KVConstants.*;

/**
 * Client API used to issue requests to key-value server.
 */
public class KVClient implements KeyValueInterface {

  public static KVMessage serverResponse = null;  // Used for testing

  public String server;
  public int port;

  /**
   * Constructs a KVClient connected to a server.
   *
   * @param server is the DNS reference to the server
   * @param port   is the port on which the server is listening
   */
  public KVClient(String server, int port) {
    this.server = server;
    this.port = port;
  }

  /**
   * This method is only used for testing server response messages.
   *
   * @param operation
   *
   * @return string representing the server response to the client request
   */
  public static String retrieveServerResponse(String operation) {
    if (operation.equals(DEL_REQ) || operation.equals(PUT_REQ)) {
      return serverResponse.getMessage();
    } else {
      return serverResponse.getKey() + " " + serverResponse.getValue();
    }
  }

  /**
   * Creates a socket connected to the server to make a request.
   *
   * @return Socket connected to server
   *
   * @throws KVException if unable to create or connect socket
   */
  public Socket connectHost() throws KVException {
    try {
      return new Socket(server, port);
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
      //System.out.println(e);
    }
  }

  /**
   * Issues a PUT request to the server.
   *
   * @param key String to put in server as key
   *
   * @throws KVException if the request was not successful in any way
   */
  @Override
  public void put(String key, String value) throws KVException {
    KVMessage request = new KVMessage(PUT_REQ);
    request.setValue(value);
    request.setKey(key);

    Socket sock = null;
    try {
      sock = connectHost();
      request.sendMessage(sock);
      KVMessage response = new KVMessage(sock);
      serverResponse = response; // For tests

      if (response.getMessage() == null
          || !response.getMessage().equals(SUCCESS)) {

        throw new KVException(response.getMessage());
      }

    } finally {
      closeHost(sock);
    }
  }

  /**
   * Issues a GET request to the server.
   *
   * @param key String to get value for in server
   *
   * @return String value associated with key
   *
   * @throws KVException if the request was not successful in any way
   */
  @Override
  public String get(String key) throws KVException {
    KVMessage request = new KVMessage(GET_REQ);
    request.setKey(key);

    Socket sock = null;
    try {
      sock = connectHost();
      request.sendMessage(sock);
      KVMessage response = new KVMessage(sock);
      serverResponse = response; // For tests

      if (response.getKey() == null || response.getKey().isEmpty()
          || response.getValue() == null || response.getValue().isEmpty()) {

        throw new KVException(response.getMessage());
      }

      return response.getValue();

    } finally {
      closeHost(sock);
    }
  }

  /**
   * Issues a DEL request to the server.
   *
   * @param key String to delete value for in server
   *
   * @throws KVException if the request was not successful in any way
   */
  @Override
  public void del(String key) throws KVException {
    KVMessage request = new KVMessage(DEL_REQ);
    request.setKey(key);

    Socket sock = null;
    try {
      sock = connectHost();
      request.sendMessage(sock);
      KVMessage response = new KVMessage(sock);
      serverResponse = response; // For tests

      if (response.getMessage() == null
          || !response.getMessage().equals(SUCCESS)) {

        throw new KVException(response.getMessage());
      }

    } finally {
      closeHost(sock);
    }
  }

}
