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

import java.util.ArrayList;
import java.util.List;

public class BinnedTally {

    public String name;
    public double binSize;
    public List<Tally> bins;

    public String toString() {
        return bins.toString();
    }

    public BinnedTally(String name) {
        this(Double.MAX_VALUE, name);
    }

    public BinnedTally(double binSize, String name) {
        this.name = name;
        this.binSize = binSize;
        this.bins = new ArrayList<Tally>();
    }

    public void tally(double sample, double value) {
        int i = (int) (sample / binSize);
        bin(i).add(value);
    }

    public void tally(double sample, Tally t) {
        int i = (int) (sample / binSize);
        while (i >= bins.size())
            bins.add(new Tally());

        bins.set(i, t);
    }

    public Tally bin(int i) {
        while (i >= bins.size())
            bins.add(new Tally());
        return bins.get(i);
    }

    public double totalObs() {
        double r = 0;
        for (Tally i : bins)
            r += i.numberObs();
        return r;
    }

    public void init() {
        for (Tally i : bins)
            i.init();
    }

    public String name() {
        return name;
    }
}