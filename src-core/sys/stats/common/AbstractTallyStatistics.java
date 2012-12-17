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
package sys.stats.common;

import java.util.LinkedList;
import java.util.List;

import swift.utils.Pair;
import umontreal.iro.lecuyer.stat.Tally;

public abstract class AbstractTallyStatistics {

    protected double valuePrecision;
    protected String sourceName;
    protected BinnedTally opsRecorder;

    private List<Pair<Long, Double>> getSumOverTime(BinnedTally bt, double timespanMillis) {
        LinkedList<Pair<Long, Double>> results = new LinkedList<Pair<Long, Double>>();
        long dT = 0;
        for (Tally t : bt.bins) {
            dT += timespanMillis;
            if (t.numberObs() != 0)
                results.add(new Pair<Long, Double>(dT, t.sum()));
        }
        return results;

    }

    // TODO: Not correct, must check
    protected List<Pair<Long, Double>> getSumOverTime(double timespanMillis) {
        assert timespanMillis > this.valuePrecision;

        BinnedTally newBinnedTally = new BinnedTally(timespanMillis, sourceName);
        long dT = 0;
        for (Tally t : this.opsRecorder.bins) {
            if (t.numberObs() != 0)
                newBinnedTally.tally(dT, t.sum());
            dT += this.valuePrecision;
        }
        return getSumOverTime(newBinnedTally, timespanMillis);

    }

    protected List<Pair<Long, Double>> getSumOverTime() {
        return getSumOverTime(opsRecorder, valuePrecision);

    }

}
