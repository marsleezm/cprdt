#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})

//Belgium = DC([ "onelab3.info.ucl.ac.be" ], [ "onelab1.info.ucl.ac.be" ]);
Belgium = DC([ "rochefort.infonet.fundp.ac.be" ], [ "orval.infonet.fundp.ac.be" ]);
//England = DC(["planetlab1.xeno.cl.cam.ac.uk"], ["planetlab2.xeno.cl.cam.ac.uk"]);

//West = DC([ "pllx1.parc.xerox.com"], ["pl-node-0.csl.sri.com", "pl-node-1.csl.sri.com"]);
//East = DC([ "planetlab1.cnds.jhu.edu"], ["planetlab2.cnds.jhu.edu"]);
//Europe = DC([ "planetlab-4.imperial.ac.uk"], ["planetlab-3.imperial.ac.uk"]);


//Texas = DC([ "ricepl-1.cs.rice.edu"], ["ricepl-2.cs.rice.edu", "ricepl-4.cs.rice.edu", "ricepl-5.cs.rice.edu"]);
//East = DC([ "planetlab2.cnds.jhu.edu"], ["planetlab2.cnds.jhu.edu"]);
//Europe = DC([ "planetlab-2.imperial.ac.uk"], ["planetlab-1.imperial.ac.uk", "planetlab-4.imperial.ac.uk"]);
 
 
//PT_Clients = SGroup( ["planetlab1.di.fct.unl.pt", "planetlab2.di.fct.unl.pt"], Europe ) 

//NV_Clients = SGroup( ["planetlab4.rutgers.edu", "planetlab3.rutgers.edu"], East)

//CA_Clients = SGroup( ["planetlab01.cs.washington.edu", "planetlab02.cs.washington.edu"], Texas)

England_Clients = SGroup( ["planetlab1.xeno.cl.cam.ac.uk", "planetlab2.xeno.cl.cam.ac.uk"], Belgium)
//Belgium_Clients = SGroup( ["onelab3.info.ucl.ac.be", "onelab1.info.ucl.ac.be"], England)
//FUNDP_Clients = SGroup( ["rochefort.infonet.fundp.ac.be", "orval.infonet.fundp.ac.be"], England)

Scouts = ( Topology.scouts() ).unique()

ShepardAddr = "vds.addictive.be"

def Threads = 3
def Duration = 300
def SwiftLinks_Props = "swiftlinks-test.props"

AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()

deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")
//deployTo(AllMachines, "stuff/loggingtest.properties", "logging.properties")
deployTo(AllMachines, SwiftLinks_Props)

def date = new Date().format('MMMdd-') + System.currentTimeMillis()

for (lazy in [false, true]) {
		
		if (lazy) {
				println "==== LAZY EXPERIMENT"
		} else {
				println "==== NONLAZY EXPERIMENT"
		}

		def shep = SwiftLinks.runShepard( ShepardAddr, Duration, "Released" )

		println "==== LAUNCHING SEQUENCERS"
		Topology.datacenters.each { datacenter ->
			datacenter.deploySequencersExtraArgs(ShepardAddr, "-rdb 1k", "512m") 
		}
		Sleep(20)
		println "==== LAUNCHING SURROGATES"
		Topology.datacenters.each { datacenter ->
			datacenter.deploySurrogatesExtraArgs(ShepardAddr, "-rdb 1k", "1900m") 
		}

		println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
		Sleep(20)

		println "==== INITIALIZING DATABASE ===="
		def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
		def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

		SwiftLinks.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftLinks_Props, "1900m")

		println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
		Sleep(20)

		SwiftLinks.runScouts( Topology.scoutGroups, SwiftLinks_Props, ShepardAddr, Threads, "1900m", lazy )

		println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
		shep.take()

		Countdown( "Remaining: ", Duration + 35)

		pnuke(Scouts, "java", 60)
		pnuke(AllMachines, "java", 60)

		def dstDir="results/swiftlinks/" + date
		def dstFile = String.format("1pc-results-swiftlinks-DC-%s-SU-%s-CL-%s-TH-%s-%s.log", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads, (lazy) ? "lazy" : "nonlazy")

		pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)
		
		def dstSubDir = String.format("1pc-stats-swiftlinks-DC-%s-SU-%s-CL-%s-TH-%s-%s", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads, (lazy) ? "lazy" : "nonlazy")
		
		pslurp( Scouts, "swiftlinks-stats", dstDir, dstSubDir, 300, false, true)
		
		Sleep(10)
}

System.exit(0)

//pssh -t 120 -i -h nodes.txt "ping -a -q -c 10 ec2-107-20-2-64.compute-1.amazonaws.com" | grep mdev | sed "s/\/ //g" | awk '{print $4}' | sed "s/\// /g" | awk '{ print $2 }' | sort
