package swift.test.microbenchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import swift.client.SwiftImpl;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.Swift;
import swift.dc.DCConstants;

public class BenchUtil {

    public static final char[] CHARS = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
            'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
            'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#', '$', '%',
            '^', '&', '*', '(', ')', '_', '-', '=', '+', '{', '}', '[', ']', '|', ':', ';', ',', '.', '?', '/', '~',
            ' ', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

    public static final int NUM_CHARS = CHARS.length;

    private static Map<String, Integer> addressPort = new HashMap<String, Integer>();

    public static String[] generateValues(int valuesSize, int valueLength, long randomSeed, double valueLengthDeviation) {
        String[] values = new String[valuesSize];
        Random rand = new Random(randomSeed);
        values = new String[valuesSize];
        for (int i = 0; i < valuesSize; i++) {
            values[i] = getRandomAString(rand, ((int) (valueLength - valueLengthDeviation)), valueLength);
        }
        return values;
    }

    private static final String getRandomAString(Random rand, int min, int max) {
        String str = new String();
        int strlen = (int) Math.floor(rand.nextDouble() * ((max - min) + 1));
        strlen += min;
        for (int i = 0; i < strlen; i++) {
            char c = CHARS[(int) Math.floor(rand.nextDouble() * NUM_CHARS)];
            str = str.concat(String.valueOf(c));
        }
        return str;
    }

    /*
     * public static void generateOperations(String[] values) {
     * BenchOperation<String>[][] operations = (BenchOperation<String>[][]) new
     * BenchOperation[values.length][2]; for (int i = 0; i < values.length; i++)
     * { operations[i][OpType.READ_ONLY.ordinal()] = new
     * BenchOperation<String>(new CRDTIdentifier( MicroBenchmark.TABLE_NAME,
     * "OBJECT" + i), values[i], OpType.READ_ONLY);
     * operations[i][OpType.UPDATE.ordinal()] = new BenchOperation<String>(new
     * CRDTIdentifier( MicroBenchmark.TABLE_NAME, "OBJECT" + i), values[(int)
     * (Math.floor(Math.random() * values.length))], OpType.UPDATE); }
     * 
     * }
     */

    public static CRDTIdentifier[] generateOpIdentifiers(int size) {
        CRDTIdentifier[] operations = new CRDTIdentifier[size];
        for (int i = 0; i < operations.length; i++) {
            operations[i] = new CRDTIdentifier(MicroBenchmark.TABLE_NAME, "OBJECT" + i);
        }
        return operations;

    }

    public static synchronized Swift getNewSwiftInterface(String serverLocation, int serverPort) {
        Integer port = addressPort.get(serverLocation);
        if (port == null) {
            port = serverPort;
            addressPort.put(serverLocation, port);
        }
        Swift client = SwiftImpl.newInstance(port++, serverLocation, DCConstants.SURROGATE_PORT);
        addressPort.put(serverLocation, port);
        return client;
    }
}
