package swift.application.reddit;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import javax.xml.bind.DatatypeConverter;

import swift.application.reddit.crdt.VoteDirection;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftSession;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

public class RedditTest {
    private static String sequencerName = "localhost";

    private static String generateClientId() {
        final UUID uuid = UUID.randomUUID();
        final byte[] uuidBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE * 2).putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits()).array();
        return DatatypeConverter.printBase64Binary(uuidBytes);
    }

    public static void main(String[] args) {
        DCSequencerServer.main(new String[] { "-name", sequencerName });
        DCServer.main(new String[] { sequencerName });

        Sys.init();
        SwiftSession clientServer = SwiftImpl.newSingleSessionInstance(new SwiftOptions("localhost",
                DCConstants.SURROGATE_PORT));

        RedditPartialReplicas client = new RedditPartialReplicas(clientServer, IsolationLevel.SNAPSHOT_ISOLATION,
                CachePolicy.STRICTLY_MOST_RECENT, generateClientId());

        client.register("Alice", "Alice", "alice@test.com");
        client.register("Bob", "Bob", "bob@test.com");

        client.login("Alice", "Alice");

        client.createSubreddit("dev");

        long now = System.currentTimeMillis();
        long date = now - (24 * 3600 * 1000);

        for (int i = 1; i <= 500; i++) {
            client.submit("link", "dev", "Post number " + i, date, "http://test.com/" + i, null);
            date += 60000; // One minute
        }

        client.logout();

        client.login("Bob", "Bob");

        List<Link> newestLinks = client.links("dev", SortingOrder.NEW, null, null, 10);

        for (Link link : newestLinks) {
            System.out.println(link);
            Vote vote = client.voteOfLink(link);
            System.out.println("Score: " + vote.getScore());
            client.voteLink(link, VoteDirection.UP);
            vote = client.voteOfLink(link);
            System.out.println("Score after vote: " + vote.getScore());
            
            client.comment(link, null, date + 10, "Test");
        }

        client.logout();

        clientServer.stopScout(true);
        System.exit(0);
    }
}
