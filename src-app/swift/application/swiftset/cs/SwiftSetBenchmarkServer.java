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
package swift.application.swiftset.cs;

import java.util.Arrays;
import java.util.logging.Logger;

import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import sys.Sys;

/**
 * 
 * 
 * @author smduarte
 * 
 */
public class SwiftSetBenchmarkServer {

    private static Logger logger = Logger.getLogger("swift.application.swiftdoc");

    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println(Arrays.asList(args));
            System.err
                    .println("Usage: [surrogate address] [server id (1|2)] [isolationLevel] [cachePolicy] [notifications (true|false)]");
            return;
        } else {
            SwiftSetServer.dcName = args[0];

            int serverId = Integer.parseInt(args[1]);
            SwiftSetServer.isolationLevel = IsolationLevel.valueOf(args[2]);
            SwiftSetServer.cachePolicy = CachePolicy.valueOf(args[3]);
            SwiftSetServer.notifications = Boolean.parseBoolean(args[4]);

            logger.info("Initializing the system");

            Sys.init();

            if (serverId == 1) {
                logger.info("Starting scout/server 1");
                SwiftSetServer.runScoutServer1();
            } else if (serverId == 2) {
                logger.info("Starting scout/server 2");
                SwiftSetServer.runScoutServer2();
            }
        }

    }
}
