package swift.test.microbenchmark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RawDataCollector {

    // [0] -> timeToexecute, [1] -> txType, [2] -> opCount, [4]
    // -> startTime
    int runCount;
    private String workerName;
    private String outputDir;
    private PrintWriter pw;
    private FileOutputStream fos;


    public RawDataCollector(int initialSize, String workerName, int runCount, String outputDir) {
        this.workerName = workerName;
        this.runCount = runCount;
        this.outputDir = outputDir;
        File file = new File(outputDir);
        if (!file.exists()) {
            file.mkdir();
        }
        String filename = "" + workerName + "_" + runCount;
        File outputFile = new File(outputDir+"/" + filename);
        try {
            fos = new FileOutputStream(outputFile);
            pw = new PrintWriter( new OutputStreamWriter( fos));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void registerOperation(long timeToexecute, int txType, int opCount,/*
                                                                               * String
                                                                               * workerId
                                                                               * ,
                                                                               * int
                                                                               * runCount
                                                                               * ,
                                                                               */
            long startTime) {
        pw.println(startTime + "\t" + ((txType == 0) ? "R" : "W") + "\t" + opCount + "\t" + timeToexecute);
    }

/*    public String RawData() {
        StringBuffer string = new StringBuffer();
        string.append("StartTime\tTxType\tOpCount\tTimeToExecute(nano)");
        string.append(workerName+"\n");
        for (int buf = 1; buf <= bufferList.size(); buf++) {
            long[][] ops = bufferList.get(buf - 1);
            int length = (buf != bufferList.size()) ? ops.length : bufferPosition;
            for (int i = 0; i < length; i++)
                string.append(ops[i][3] + "\t" + ((ops[i][1] == 0) ? "R" : "W") + "\t" + ops[i][2] + "\t" + ops[i][0]
                        + "\n");
        }
        return string.toString();
    }
*/
    public int getRunCount() {
        return runCount;
    }

    public String getWorkerName() {
        return workerName;
    }

    public void rawDataToFile() {
        try {
            pw.flush();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
