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
package sys.stats;

import java.util.HashMap;
import java.util.Map;

import sys.net.api.Endpoint;
import sys.scheduler.PeriodicTask;
import umontreal.iro.lecuyer.stat.Tally;

import static sys.Sys.*;

public class TcpStats {

	Map<Endpoint, Tally> rpcRTT = new HashMap<Endpoint, Tally>();

	protected TcpStats() {
	}

	synchronized public void logRpcRTT(Endpoint dst, double rtt) {
		Tally tally = rpcRTT.get(dst);
		if (tally == null)
			rpcRTT.put(dst, tally = new Tally(Sys.mainClass + " TCP RTT: " + dst));

		tally.add(rtt * 1000);
	}

	static {
		TcpStats = new TcpStats();
		// new PeriodicTask(0, 30) {
		// public void run() {
		// synchronized (TcpStats) {
		// if (TcpStats.rpcRTT.size() > 0) {
		// for (Tally i : TcpStats.rpcRTT.values()) {
		// if (i.numberObs() > 2)
		// System.err.println(i.report());
		// }
		// }
		// }
		// }
		// };
	}
	public static TcpStats TcpStats;
}
