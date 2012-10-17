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

import java.net.Socket;
import swift.dc.DCSequencerServer;
import swift.dc.DCServer;
import sys.Sys;

/**
 *
 * @author Stephane Martin <stephane.martin@loria.fr>
 */
public class SwiftCloudServerJob extends Jobs {
    private  String sequencerName = "localhost";
    @Override
    public void run() {
        DCSequencerServer.main(new String[]{"-name", sequencerName});

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // do nothing
        }

        DCServer.main(new String[]{sequencerName});
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        Sys.init();
    }
    
}
