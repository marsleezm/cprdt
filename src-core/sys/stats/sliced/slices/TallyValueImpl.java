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
package sys.stats.sliced.slices;

import sys.stats.sliced.SlicedStatistics;
import umontreal.iro.lecuyer.stat.Tally;

public class TallyValueImpl implements SlicedStatistics<TallyValueImpl> {

    private Tally value;

    public TallyValueImpl() {
        value = new Tally();
    }

    public void addValue(double value) {
        this.value.add(value);
    }

    public int getTotalOperations() {
        return this.value.numberObs();
    }

    public double getSumValue() {
        return this.value.sum();
    }

    public double getAvgValue() {
        return this.value.average();
    }

    public TallyValueImpl createNew() {
        return new TallyValueImpl();
    }

}
