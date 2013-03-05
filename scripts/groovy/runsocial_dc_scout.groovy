#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


Surrogates = [
    "ec2-54-246-29-95.eu-west-1.compute.amazonaws.com"
]

int maxPerRegion = 32

EndClients_EU = [
    "planetlab-node-02.ucd.ie",
    "planetlab-node-01.ucd.ie",
    "planetlab-tea.ait.ie",
    "planetlab-coffee.ait.ie",
    "planetlabeu-1.tssg.org",
    "planetlabeu-2.tssg.org",
    "planetlab-2.imperial.ac.uk",
    "planetlab-1.imperial.ac.uk",
    "planetlab-4.imperial.ac.uk",
    "planetlab1.xeno.cl.cam.ac.uk",
    "planetlab3.xeno.cl.cam.ac.uk",
    "planetlab1.aston.ac.uk",
    "planetlab2.xeno.cl.cam.ac.uk",
    "planetlab-3.imperial.ac.uk",
    "planetlab3.cs.st-andrews.ac.uk",
    "planetlab4.cs.st-andrews.ac.uk",
    "planetlab2.aston.ac.uk",
//    "pl1.ccsrfi.net",
//    "pl2.ccsrfi.net",
//    "onelab2.info.ucl.ac.be",
    "planetlab2.cs.vu.nl",
//    "planetlab2.ewi.tudelft.nl",
    "planck228ple.test.ibbt.be",
    "planetlab1.cs.vu.nl",
    "chimay.infonet.fundp.ac.be",
    "rochefort.infonet.fundp.ac.be",
    "planet2.l3s.uni-hannover.de",
    "planet1.l3s.uni-hannover.de",
    "planetlab2.extern.kuleuven.be",
    "orval.infonet.fundp.ac.be",
    "planck227ple.test.ibbt.be",
    "planetlab2.montefiore.ulg.ac.be",
    "plewifi.ipv6.lip6.fr",
    "ple2.ipv6.lip6.fr",
//    "ple3.ipv6.lip6.fr",
    "host3-plb.loria.fr",
    "ple4.ipv6.lip6.fr",
    "ple6.ipv6.lip6.fr",
    "planetlab1.montefiore.ulg.ac.be",
    "peeramide.irisa.fr",
    "host4-plb.loria.fr",
    "inriarennes1.irisa.fr"
    ].subList(0, maxPerRegion)

EndClients_NC = [
    "pli1-pa-6.hpl.hp.com",
    "pli1-pa-4.hpl.hp.com",
    "planetslug4.cse.ucsc.edu",
    "planetslug5.cse.ucsc.edu",
    "pli1-pa-5.hpl.hp.com",
    "planetlab1.millennium.berkeley.edu",
    "planetlab2.millennium.berkeley.edu",
    "planetlab8.millennium.berkeley.edu",
    "planetlab7.millennium.berkeley.edu",
    "planetlab12.millennium.berkeley.edu",
    "planetlab5.millennium.berkeley.edu",
    "planetlab16.millennium.berkeley.edu",
//    "planetlab10.millennium.berkeley.edu",
//    "planetlab13.millennium.berkeley.edu",
    "pl-node-0.csl.sri.com",
    "pl-node-1.csl.sri.com",
    "planetlab-1.calpoly-netlab.net",
    "planetlab-2.calpoly-netlab.net",
    "planet3.cs.ucsb.edu",
    "planet4.cs.ucsb.edu",
//    "planet5.cs.ucsb.edu",
    "planet6.cs.ucsb.edu",
    "planetlab1.cs.ucla.edu",
    "planetlab2.cs.ucla.edu",
    "planetlab1.postel.org",
    "planetlab2.postel.org",
    "planetlab3.postel.org",
    "planetlab4.postel.org",
    "planetlab1.ucsd.edu",
    "planetlab2.ucsd.edu",
    "planetlab3.ucsd.edu",
//    "pllx1.parc.xerox.com",
//    "pllx2.parc.xerox.com",
    "planetlab01.cs.washington.edu",
    "planetlab02.cs.washington.edu",
    "planetlab03.cs.washington.edu",
    "planetlab04.cs.washington.edu",
    "planetlab6.flux.utah.edu",
    "planetlab7.flux.utah.edu",
    "planetlab1.byu.edu",
    "planetlab2.byu.edu",
    "planetlab2.cs.ubc.ca",
    "planetlab1.cs.uoregon.edu",
    "planetlab2.cs.uoregon.edu",
    "planetlab3.cs.uoregon.edu",
    "planetlab4.cs.uoregon.edu"
//    "pl2.cs.unm.edu",
//    "pl3.cs.unm.edu",
//    "pl4.cs.unm.edu",
].subList(0, maxPerRegion)

EndClients_NV = [
    "planetlab-2.cmcl.cs.cmu.edu",
    "planetlab-3.cmcl.cs.cmu.edu",
    "planetlab1.cs.pitt.edu",
    "planetlab2.cs.pitt.edu",
    "planetlab-1.cmcl.cs.cmu.edu",
    "planetlab1.poly.edu",
    "planetlab2.poly.edu",
//    "planetlab-01.cs.princeton.edu",
    "planetlab-03.cs.princeton.edu",
    "planetlab1.temple.edu",
    "planetlab3.cs.columbia.edu",
    "planetlab2.cs.columbia.edu",
    "planetlab7.cs.duke.edu",
    "pl1.cs.yale.edu",
    "pl2.cs.yale.edu",
    "planetlab-4.eecs.cwru.edu",
    "planetlab-5.eecs.cwru.edu",
    "planetlab1.cs.unc.edu",
    "planetlab2.cs.unc.edu",
    "planetlab-01.vt.nodes.planet-lab.org",
    "planetlab-02.vt.nodes.planet-lab.org",
    "planetlab-03.vt.nodes.planet-lab.org",
    "planetlab-04.vt.nodes.planet-lab.org",
//    "planetlab-04.cs.princeton.edu",
    "node2.planetlab.albany.edu",
    "node1.planetlab.albany.edu",
    "planetlab6.cs.duke.edu",
    "planetlab-01.bu.edu",
    "planetlab-02.bu.edu",
    "planetlab4.csail.mit.edu",
    "planetlab6.csail.mit.edu",
    "righthand.eecs.harvard.edu",
    "lefthand.eecs.harvard.edu",
    "earth.cs.brown.edu",
    "planetlab2.temple.edu",
    "planetlab1.cs.umass.edu",
    "planetlab2.cs.umass.edu",
    "planetlab4.williams.edu",
    "planetlab5.williams.edu",
    "planetlab1.cs.uml.edu",
    "planetlab1.clemson.edu",
    "planetlab2.clemson.edu",
    "planetlab1.umassd.edu",
    "planetlab2.umassd.edu",
    "planetlab1.cnis.nyit.edu",
    "planetlab1.cs.cornell.edu",
    "planetlab2.cs.cornell.edu",
    "planetlab5.cs.cornell.edu",
    "planetlab6.cs.cornell.edu",
//    "node-2.mcgillplanetlab.org",
    "planetlab2.williams.edu",
//    "node-1.mcgillplanetlab.org",
    "jupiter.cs.brown.edu",
    "planetlab2.cnis.nyit.edu",
    "miranda.planetlab.cs.umd.edu",
//    "planetlab01.alucloud.com",
//    "planetlab02.alucloud.com",
    "plnode-04.gpolab.bbn.com",
    "plnode-03.gpolab.bbn.com",
    "planetlab1.mnlab.cti.depaul.edu",
    "planetlab-2.cs.uic.edu",
    "planet2.cc.gt.atl.ga.us",
    "planet4.cc.gt.atl.ga.us",
    "planetlab2.mnlab.cti.depaul.edu",
    "pl2.csl.utoronto.ca",
    "planetlab3.eecs.northwestern.edu",
//    "planetlab1.citadel.edu",
//    "planetlab2.citadel.edu",
    "planetlab2.eecs.ucf.edu",
    "planetlab1.eecs.ucf.edu",
    "saturn.planetlab.carleton.ca",
    "planetlab4.cs.uiuc.edu",
    "planetlab1.cs.uiuc.edu",
    "planetlab6.cs.uiuc.edu",
    "planetlab5.cs.uiuc.edu",
    "planetlab4.wail.wisc.edu"
    ].subList(0, maxPerRegion)

EndClients_PT = [
    "planetlab1.di.fct.unl.pt",
    "planetlab2.di.fct.unl.pt"    
    ]

//EndClients_EU = []
//EndClients_NC = []

EndClients = EndClients_NC + EndClients_NV + EndClients_EU

Shepard = Surrogates.get(0);

def Threads = 3
def Duration = 600
def SwiftSocial_Props = "swiftsocial-test.props"

def Scouts = Surrogates

AllMachines = (Surrogates + EndClients + Shepard).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

println "==== BUILDING JAR..."

sh("ant -buildfile smd-jar-build.xml").waitFor()

deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")
deployTo(AllMachines, SwiftSocial_Props)


def shep = SwiftSocial.runShepard( Shepard, Duration, "Released" )

SwiftSocial.runEachAsDatacentre(Surrogates, "256m", "1024m")

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(10)


println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Surrogates.get(0)
def INIT_DB_CLIENT = Surrogates.get(0)

SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SERVER SCOUTS ===="
Sleep(10)

SwiftSocial.runCS_ServerScouts( Scouts, ["localhost"], SwiftSocial_Props, "-cache 0", "2048m")

println "==== WAITING A BIT BEFORE STARTING ENDCLIENTS ===="
Sleep(10)

SwiftSocial.runCS_EndClients( EndClients, Scouts, SwiftSocial_Props, Shepard, Threads )


println "==== WAITING FOR SHEPARD TO INITIATE COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), EndClients.size(), Threads)

pslurp( EndClients, "client-stdout.txt", dstDir, dstFile, 300)

exec(["/bin/bash", "-c", "wc " + dstDir + "/*/*"]).waitFor()

System.exit(0)

