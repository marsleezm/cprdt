/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.test.microbenchmark;

import java.util.Random;

import swift.test.microbenchmark.interfaces.MicroBenchmarkWorker;
import swift.test.microbenchmark.interfaces.ResultHandler;
import swift.test.microbenchmark.interfaces.WorkerManager;
import sys.net.impl.KryoSerializer;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakRetryFailedException;
import com.esotericsoftware.kryo.Kryo;

public class RiakInitializerWorker implements MicroBenchmarkWorker {

    private WorkerManager manager;
    private IRiakClient clientServer;
    private Random random;
    private Integer[] identifiers;
    private Kryo kryo;

    protected long startTime, endTime;

    public RiakInitializerWorker(WorkerManager manager, Integer[] identifiers, Random random, IRiakClient clientServer) {
        this.manager = manager;
        this.identifiers = identifiers;
        this.random = random;
        this.clientServer = clientServer;
        this.kryo = new Kryo();

    }

    @Override
    public void run() {
        manager.onWorkerStart(this);
        startTime = System.currentTimeMillis();

        KryoSerializer serializer = new KryoSerializer();
        try {
            clientServer.createBucket(RiakMicroBenchmark.TABLE_NAME).allowSiblings(false).execute();
            for (Integer i : identifiers) {
                byte[] data = serializer.writeObject(i);
                clientServer.fetchBucket(RiakMicroBenchmark.TABLE_NAME).execute().store("object" + i, data).execute();
                // byte[] b =
                // clientServer.fetchBucket(RiakMicroBenchmark.TABLE_NAME).execute().fetch("object"
                // + i).execute().getValue();
                // ByteBuffer bb = ByteBuffer.wrap(b);
                // System.out.println(kryo.readObject(bb, Integer.class));;
            }
        } catch (RiakRetryFailedException e) {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
        manager.onWorkerFinish(this);

    }

    @Override
    public ResultHandler getResults() {
        return new RiakInitializerResultHandler(this);
    }

    @Override
    public void stop() {

    }

    @Override
    public String getWorkerID() {
        return "INITIALIZER";
    }

    @Override
    public RawDataCollector getRawData() {
        return null;
    }
}

class RiakInitializerResultHandler implements ResultHandler {

    private double executionTime;

    public RiakInitializerResultHandler(RiakInitializerWorker worker) {
        executionTime = (worker.endTime - worker.startTime);
    }

    @Override
    public String toString() {
        String results = "***DB initializer Results***\n";
        results += "Execution Time:\t" + executionTime / 1000d + "s";
        return results;
    }

    @Override
    public double getExecutionTime() {
        return executionTime;
    }

    @Override
    public int getNumExecutedTransactions() {
        return 0;
    }

    @Override
    public int getWriteOps() {
        return 0;
    }

    @Override
    public int getReadOps() {
        return 0;
    }

    @Override
    public String getWorkerID() {
        return "RIAKINITIALIZER";
    }

    /*
     * @Override public String getRawResults() { return "INITIALIZER"; }
     */
}
