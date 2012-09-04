package sys.net.api;

/**
 * Represents a connection to a remote endpoint, which can be used to exchange
 * messages.
 * 
 * @author smd
 * 
 */
public interface TransportConnection {

	/**
	 * Obtains the state of the connection. Connections may fail to be
	 * established or fail during message exchange.
	 * 
	 * @return true if the connection has failed to be established or has failed
	 *         since; false otherwise
	 */
	boolean failed();

	/**
	 * Disposes of this connection.
	 */
	void dispose();

	/**
	 * Sends a message using this connection
	 * 
	 * @param m
	 *            the message being sent
	 * @return false if an error occurred; true if no error occurred.
	 */
	boolean send(final Message m);

	/**
	 * Sends a message using this connection, as soon as possible...
	 * 
	 * @param m
	 *            the message being sent
	 * @return false if an error occurred; true if no error occurred.
	 */
	boolean sendNow(final Message m);

	
	/**
	 * Blocks until a message is received from this connection
	 * 
	 * @return the message received
	 */
	<T extends Message> T receive();

	/**
	 * Obtains the local endpoint for this connection
	 * 
	 * @return the local endpoint associated with this connection
	 */
	Endpoint localEndpoint();

	/**
	 * Obtains the remote endpoint for this connection
	 * 
	 * @return the remote endpoint associated with this connection
	 */
	Endpoint remoteEndpoint();

	/**
	 * If the connection has failed due to an exception, this method provides the cause of failure.
	 * @return the cause for the connection failure.
	 */
	Throwable causeOfFailure();	
	
	/**
	 * 
	 */
	void setOption(String op, Object value);
}