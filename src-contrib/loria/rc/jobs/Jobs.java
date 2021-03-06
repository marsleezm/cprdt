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
package loria.rc.jobs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public abstract class Jobs implements Serializable, Runnable {

    protected boolean run = true;
    protected Thread th;
    protected String jobName = "";
    protected String destHostName = "";
    protected ObjectOutputStream outObj;

    public void doOperation(ObjectOutputStream s) {
        this.outObj = s;
        th = new Thread(this);
        th.start();
    }

    void stop() {
        run = false;
    }

    public boolean isRunning() {
        return run;
    }

    public void sendObejct(Object obj) {
        if (outObj == null) {
            Logger.getLogger(Jobs.class.getName()).log(Level.INFO, null, obj);
        } else {
            try {
                outObj.writeObject(obj);
            } catch (IOException ex) {
                Logger.getLogger(Jobs.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getDestHostName() {
        return destHostName;
    }

    public void setDestHostName(String destHostName) {
        this.destHostName = destHostName;
    }

    public void join() throws InterruptedException {
        th.join();
    }
}
