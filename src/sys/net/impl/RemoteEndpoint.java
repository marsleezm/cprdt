package sys.net.impl;

import java.net.InetSocketAddress;

import sys.net.api.Endpoint;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.NetworkingException;
import sys.net.api.TransportConnection;

/**
 * 
 * Represents a remote endpoint location. Used as a destination for sending
 * messages.
 * 
 * @author smd
 * 
 */
public class RemoteEndpoint extends AbstractEndpoint {

	public RemoteEndpoint(final String host, final int tcpPort) {
		super(new InetSocketAddress(host, tcpPort), 0);
	}

	public RemoteEndpoint(long locator, long gid) {
		super(locator, gid);
	}

	@Override
	public <T extends Endpoint> T setHandler(MessageHandler handler) {
		throw new NetworkingException("Not supported...[This is a remote endpoint...]");
	}

	@Override
	public TransportConnection send(final Endpoint dst, final Message m) {
		throw new NetworkingException("Not supported...[This is a remote endpoint...]");
	}

	@Override
	public TransportConnection connect(Endpoint dst) {
		throw new NetworkingException("Not supported...[This is a remote endpoint...]");
	}
}
