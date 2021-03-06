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
package swift.dc.db;

import static sys.net.api.Networking.Networking;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.crdt.core.CRDTIdentifier;
import swift.dc.CRDTData;
import swift.dc.DCConstants;
import sys.utils.FileUtils;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;
import com.sleepycat.db.TransactionConfig;

public class DCBerkeleyDBDatabase implements DCNodeDatabase {
    static Logger logger = Logger.getLogger(DCBerkeleyDBDatabase.class.getName());
    Environment env;
    Map<String, Database> databases;
    File dir;
    TransactionConfig txnConfig;

    public DCBerkeleyDBDatabase() {
    }

    /*
     * DatabaseConfig dbConfig = new DatabaseConfig();
     * dbConfig.setTransactional(true); dbConfig.setAllowCreate(true);
     * dbConfig.setType(DatabaseType.BTREE);
     * 
     * Iterator<KVExecutionPolicy> it = config.getPolicies().iterator(); while(
     * it.hasNext()) { KVExecutionPolicy pol = it.next(); String tableName =
     * pol.getTableName();
     * 
     * Database db = env.openDatabase(null, // txn handle tableName, // db file
     * name tableName, // db name dbConfig);
     */

    @Override
    public void init(Properties props) {
        try {
            databases = new HashMap<String, Database>();

            String dirPath = props.getProperty(DCConstants.BERKELEYDB_DIR);

            dir = new File(dirPath);
            if (dir.exists())
                FileUtils.deleteDir(dir);
            dir.mkdirs();

            EnvironmentConfig myEnvConfig = new EnvironmentConfig();
            myEnvConfig.setInitializeCache(true);
            myEnvConfig.setInitializeLocking(true);
            myEnvConfig.setInitializeLogging(true);
            myEnvConfig.setTransactional(true);
            // myEnvConfig.setMultiversion(true);
            myEnvConfig.setAllowCreate(true);

            env = new Environment(dir, myEnvConfig);

            txnConfig = new TransactionConfig();
            txnConfig.setSnapshot(true);

        } catch (FileNotFoundException e) {
            throw new RuntimeException("Cannot create databases", e);
        } catch (DatabaseException e) {
            throw new RuntimeException("Cannot create databases", e);
        } catch (Error e) {
            throw new RuntimeException(
                    "Cannot create databases - maybe you are forgetting to include library in path.\n"
                            + "Try something like: -Djava.library.path=lib/build_unix/.libs to java command", e);

        }
    }

    private Database getDatabase(String tableName) {
        Database db = null;
        synchronized (databases) {
            db = databases.get(tableName);
            if (db == null) {
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setTransactional(true);
                dbConfig.setAllowCreate(true);
                dbConfig.setType(DatabaseType.HASH);

                try {
                    db = env.openDatabase(null, // txn handle
                            tableName, // db file name
                            tableName, // db name
                            dbConfig);
                    databases.put(tableName, db);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Cannot create database : " + tableName, e);
                } catch (DatabaseException e) {
                    throw new RuntimeException("Cannot create database : " + tableName, e);
                }
            }
            return db;
        }
    }

    @Override
    public synchronized CRDTData<?> read(CRDTIdentifier id) {
        return (CRDTData<?>) readSysData(id.getTable(), id.getKey());
    }

    @Override
    public synchronized boolean write(CRDTIdentifier id, CRDTData<?> data) {
        return writeSysData(id.getTable(), id.getKey(), data);
    }

    @Override
    public boolean ramOnly() {
        return env == null;
    }

    @Override
    public synchronized Object readSysData(String table, String key) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("DCBerkeleyDBDatabase: get: " + table + ";" + key);
        }
        Database db = getDatabase(table);

        Transaction tx = null;
        try {
            tx = env.beginTransaction(null, txnConfig);

            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry();

            // Perform the get.
            if (db.get(tx, theKey, theData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                return Networking.serializer().readObject(theData.getData());
            } else
                return null;
        } catch (IOException e) {
            logger.throwing("DCBerkeleyDBDatabase", "get", e);
            return null;
        } catch (DatabaseException e) {
            logger.throwing("DCBerkeleyDBDatabase", "get", e);
            return null;
        } finally {
            if (tx != null)
                try {
                    tx.commitNoSync();
                } catch (DatabaseException e) {
                    logger.throwing("DCBerkeleyDBDatabase", "get", e);
                }
        }
    }

    @Override
    public synchronized boolean writeSysData(String table, String key, Object data) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("DCBerkeleyDBDatabase: put: " + table + ";" + key);
        }
        Database db = getDatabase(table);

        Transaction tx = null;
        try {
            tx = env.beginTransaction(null, txnConfig);

            byte[] arr = Networking.serializer().writeObject(data);

            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry(arr);

            // Perform the put
            if (db.put(tx, theKey, theData) == OperationStatus.SUCCESS) {
                return true;
            } else
                return false;
        } catch (IOException e) {
            logger.throwing("DCBerkeleyDBDatabase", "put", e);
            return false;
        } catch (DatabaseException e) {
            logger.throwing("DCBerkeleyDBDatabase", "put", e);
            return false;
        } finally {
            if (tx != null)
                try {
                    tx.commitNoSync();
                } catch (DatabaseException e) {
                    logger.throwing("DCBerkeleyDBDatabase", "put", e);
                }
        }
    }

}
