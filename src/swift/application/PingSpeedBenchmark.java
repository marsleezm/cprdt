package swift.application;

import java.util.logging.Logger;

import swift.client.SwiftImpl;
import swift.dc.DCConstants;
import sys.Sys;

/**
 * Local setup/test with one server and two clients.
 * 
 * @author annettebieniusa
 * 
 */
public class PingSpeedBenchmark {
    private static String dcName;
    private static int clientId;
    private static int portId;
    private static Logger logger = Logger.getLogger("swift.application");

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: [number of iterations] [client id (1|2)] [sequencer] [port]");
            return;
        } else {
            PingSpeedTest.iterations = Integer.parseInt(args[0]);
            clientId = Integer.parseInt(args[1]);
            dcName = args[2];
            portId = Integer.parseInt(args[3]);
        }

        logger.info("Initializing the system");
        Sys.init();
        SwiftImpl clientServer = SwiftImpl.newInstance(portId, dcName, DCConstants.SURROGATE_PORT);

        if (clientId == 1) {
            logger.info("Starting client 1");
            PingSpeedTest.client1Code(clientServer);
        } else if (clientId == 2) {
            logger.info("Starting client 2");
            PingSpeedTest.client2Code(clientServer);
        }
        clientServer.stop(true);
    }
}
