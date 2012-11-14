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
package swift.application.swiftset;

import java.util.Arrays;
import java.util.logging.Logger;

import swift.client.SwiftImpl;
import swift.client.SwiftOptions;
import swift.crdt.interfaces.CachePolicy;
import swift.crdt.interfaces.IsolationLevel;
import swift.crdt.interfaces.SwiftSession;
import swift.dc.DCConstants;
import sys.Sys;

/**
 * 
 * 
 * @author smduarte, annettebieniusa
 * 
 */
public class SwiftDocBenchmark {
	private static String dcName;
	private static int dcPort;
	private static int clientId;
	private static Logger logger = Logger.getLogger("swift.application");

	public static void main(String[] args) throws Exception {
		
		if (args.length != 6) {
			System.out.println( "-->" + Arrays.asList( args )) ;
			System.out.println("Usage: [surrogate address] [number of iterations] [client id (1|2)] [isolationLevel] [cachePolicy] [notifications (true|false)]");
			return;
		} else {
			dcName = args[0];
			int pos = dcName.indexOf(":");
			if (pos != -1) {
				dcPort = Integer.parseInt(dcName.substring(pos + 1));
				dcName = dcName.substring(0, pos);
			} else
				dcPort = DCConstants.SURROGATE_PORT;
			SwiftSet.iterations = Integer.parseInt(args[1]);
			clientId = Integer.parseInt(args[2]);
			SwiftSet.isolationLevel = IsolationLevel.valueOf(args[3]);
			SwiftSet.cachePolicy = CachePolicy.valueOf(args[4]);
			SwiftSet.notifications = Boolean.parseBoolean(args[5]);
		}

		logger.info("Initializing the system");
		
		Sys.init();
		
        SwiftSession swift1 = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName, dcPort));
        SwiftSession swift2 = SwiftImpl.newSingleSessionInstance(new SwiftOptions(dcName, dcPort));

		if (clientId == 1) {
			logger.info("Starting client 1");
			SwiftSet.runClient1(  swift1, swift2);
		} else if (clientId == 2) {
			logger.info("Starting client 2");
			SwiftSet.runClient2( swift1, swift2);
		}

//		Threading.newThread(true, new Runnable() {
//
//			@Override
//			public void run() {
//				// TODO Auto-generated method stub
//				SwiftDoc.runClient2( SwiftImpl.newInstance(dcName, dcPort) );				
//			}
//			
//		}).start();
//		SwiftDoc.runClient1( SwiftImpl.newInstance(dcName, dcPort) );		
	}
}
