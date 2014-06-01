package swift.client;

import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.ReturnableTimestampSourceDecorator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.crdt.core.CachePolicy;
import swift.crdt.core.IsolationLevel;
import swift.crdt.core.SwiftScout;
import swift.crdt.core.SwiftSession;
import swift.crdt.core.TxnHandle;
import swift.utils.DummyLog;
import swift.utils.TransactionsLog;
import sys.stats.DummyStats;
import sys.stats.Stats;

public class SwiftSessionTester implements SwiftSession {
    TxnManagerTester manager;
    TransactionsLog durableLog;
    ReturnableTimestampSourceDecorator<Timestamp> clientTimestampGenerator;
    Stats stats;
    
    public SwiftSessionTester() {
        manager = new TxnManagerTester();
        durableLog = new DummyLog();
        this.clientTimestampGenerator = new ReturnableTimestampSourceDecorator<Timestamp>(
                new IncrementalTimestampGenerator("unique"));
        this.stats = new DummyStats();
    }

    @Override
    public TxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cachePolicy, boolean readOnly) {
        if (readOnly) {
            return new SnapshotIsolationTxnHandle(manager, getSessionId(), cachePolicy, manager.getLatestClock(), stats);
        } else {
            return new SnapshotIsolationTxnHandle(manager, getSessionId(), durableLog, cachePolicy,
                    generateNextTimestampMapping(), manager.getLatestClock(), stats);
        }
    }
    
    public TxnHandle beginTxn(boolean readOnly) {
        return beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.CACHED, readOnly);
    }

    private TimestampMapping generateNextTimestampMapping() {
        return new TimestampMapping(clientTimestampGenerator.generateNew());
    }

    @Override
    public void stopScout(boolean waitForCommit) {
    }

    @Override
    public String getSessionId() {
        return "1";
    }

    @Override
    public SwiftScout getScout() {
        return null;
    }

    @Override
    public void printStatistics() {
    }
}
