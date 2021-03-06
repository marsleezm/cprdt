/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2012 University of Kaiserslautern
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
/*
 *  Replication Benchmarker
 *  https://github.com/score-team/replication-benchmarker/
 *  Copyright (C) 2012 LORIA / Inria / SCORE Team
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */
package swift.application.filesystem.fuse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.junit.Test;

import swift.application.filesystem.Filesystem;
import swift.application.filesystem.FilesystemBasic;
import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.crdt.interfaces.TxnHandle;
import swift.dc.DCConstants;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;
import fuse.FuseGetattrSetter;

/**
 * 
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class FilesystemFuseTest {

    public FilesystemFuseTest() {
    }

    private static String sequencerName = "localhost";
    private static String scoutName = "localhost";
    private static Logger logger = Logger.getLogger("swift.filesystem.fuse");

    @Test
    public void testSomeMethod() throws Exception {
        DCSequencerServer.main(new String[] { "-name", sequencerName });

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }

        DCServer.main(new String[] { sequencerName });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        Sys.init();
        SwiftSession server = SwiftImpl
                .newSingleSessionInstance(new SwiftOptions(scoutName, DCConstants.SURROGATE_PORT));

        TxnHandle txn;
        // try {
        txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION, CachePolicy.STRICTLY_MOST_RECENT, false);

        // create a root directory
        logger.info("Creating file system");
        Filesystem fs = new FilesystemBasic(txn, "test", "DIR");
        txn.commit();

        FilesystemFuse.initServerInfrastructure("localhost", "localhost");
        FilesystemFuse fsf = new FilesystemFuse();
        fsf.fs = fs;

        FuseGetattrSetter getattrSetterMock = new FuseGetattrSetter() {
            @Override
            public void set(long l, int i, int i1, int i2, int i3, int i4, long l1, long l2, int i5, int i6, int i7) {
                // throw new
                // UnsupportedOperationException("Not supported yet.");
                System.out.println("l:" + l + " i:" + i + " i1:" + i1 + " i2:" + i2 + " i3:" + i3 + " i4:" + i4
                        + " l1:" + l1 + " i5:" + i5 + " i6:" + i6 + " i7:" + i7);
            }
        };

        logger.info("Creating directories and subdirectories");
        // txn = server.beginTxn(IsolationLevel.SNAPSHOT_ISOLATION,
        // CachePolicy.STRICTLY_MOST_RECENT, false);

        fsf.mkdir("/test", 511);
        assertEquals(0, fsf.getattr("/test", getattrSetterMock));

        logger.info("Creating a file");

        String filename = "/test/file1.txt";
        fsf.mknod(filename, 511, 0);
        assertEquals(0, fsf.getattr("/test", getattrSetterMock));
        assertEquals(0, fsf.getattr(filename, getattrSetterMock));
        FuseOpeenSetterMock fos = new FuseOpeenSetterMock();
        fsf.open(filename, 0, fos);

        String s = "This is a test file";

        fsf.write(filename, fos.getFile(), false, ByteBuffer.wrap(s.getBytes()), 0);

        fsf.flush(filename, fos.getFile());
        // txn.commit();

        fsf.open(filename, 0, fos);
        ByteBuffer buff = ByteBuffer.allocate(30);
        fsf.read(filename, fos.getFile(), buff, 0);

        assertTrue(new String(buff.array()).startsWith(s));

        assertEquals(0, fsf.getattr(filename, getattrSetterMock));

        fsf.unlink(filename);

        fsf.mknod(filename, 511, 0);
        assertEquals(0, fsf.getattr("/test", getattrSetterMock));
        assertEquals(0, fsf.getattr(filename, getattrSetterMock));

        fsf.open(filename, 0, fos);

        s = "This is a test file 33";

        fsf.write(filename, fos.getFile(), false, ByteBuffer.wrap(s.getBytes()), 0);

        fsf.flush(filename, fos.getFile());
        // txn.commit();

        fsf.open(filename, 0, fos);
        ByteBuffer buff2 = ByteBuffer.allocate(30);
        fsf.read(filename, fos.getFile(), buff2, 0);

        assertTrue(new String(buff2.array()).startsWith(s));

        assertEquals(0, fsf.getattr(filename, getattrSetterMock));

    }
}
