#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy "$0" $@; exit $?

package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.Topology.*;


def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


EuropeEC2 = [
    'ec2-54-77-120-200.eu-west-1.compute.amazonaws.com'
]

EuropePlanetLab = [
    'rochefort.infonet.fundp.ac.be',
    'orval.infonet.fundp.ac.be',
    'planetlab3.xeno.cl.cam.ac.uk',
    'planetlab-tea.ait.ie',
    'planetlab2.xeno.cl.cam.ac.uk',
    'onelab3.info.ucl.ac.be',
    'inriarennes2.irisa.fr',
    'planetlab1.xeno.cl.cam.ac.uk',
    'peeramidion.irisa.fr',
    'planetlab-coffee.ait.ie'
]

NorthVirginiaEC2 = [
    'ec2-54-210-244-16.compute-1.amazonaws.com'
]

NorthVirginiaPlanetLab = [
//    'planetlab01.uncc.edu', // No response
    'planetlab2.netlab.uky.edu', // Low mem
    'planetlab1.netlab.uky.edu', // Low mem
//    'planetlab1.georgetown.edu', // No response
//    'planetlab2.georgetown.edu', // No reponse
    'node1.planetlab.mathcs.emory.edu', // Low mem
    'node2.planetlab.mathcs.emory.edu', // Low mem
    'planetlab2.jhu.edu', // Low mem
    'planetlab1.jhu.edu', // Low mem
    'planetlab3.georgetown.edu', // Ok
    'planetlab-01.vt.nodes.planet-lab.org', // Low mem
    'planetlab-03.vt.nodes.planet-lab.org' // Low mem
]

OregonEC2 = [
    'ec2-54-191-40-131.us-west-2.compute.amazonaws.com'
]

OregonPlanetlab = [
    'planetlab1.unr.edu', // Ok
    'planetlab2.unr.edu', // Ok
//    'planetlab4.flux.utah.edu', // Refused
//    'planetlab5.flux.utah.edu', // Refused
//    'planetlab6.flux.utah.edu', // Time out
//    'planetlab7.flux.utah.edu', // Time out
//    'planetlab2.eecs.wsu.edu', // User does not exist
    'planetlab1.eecs.wsu.edu', // Low mem
//    'pl3.planetlab.uvic.ca', // Name unknown
    'planetlab1.cs.uoregon.edu', // Low mem
    'planetlab4.cs.uoregon.edu', // Low mem
    'planetlab02.cs.washington.edu' // Very low mem
]

if (args.length < 3) {
    System.err.println "usage: runlinks.groovy <per DC client limit> <threads per scout> [paths to property files...]"
    System.exit(1)
}
PerDCClientNodesLimit = Integer.valueOf(args[0])
Threads = Integer.valueOf(args[1])

Europe = DC([EuropeEC2[0]], [EuropeEC2[0], EuropeEC2[0]])
NorthVirginia = DC([NorthVirginiaEC2[0]], [NorthVirginiaEC2[0], NorthVirginiaEC2[0]])
Oregon = DC([OregonEC2[0]], [OregonEC2[0], OregonEC2[0]])

ScoutsEU = SGroup( EuropePlanetLab[0..(Math.min(EuropePlanetLab.size(), PerDCClientNodesLimit)-1)], Europe )
ScoutsNorthVirginia = SGroup(NorthVirginiaPlanetLab[0..(Math.min(NorthVirginiaPlanetLab.size(), PerDCClientNodesLimit)-1)], NorthVirginia )
ScoutsOregon = SGroup(OregonPlanetlab[0..(Math.min(OregonPlanetlab.size(), PerDCClientNodesLimit)-1)], Oregon )


Scouts = ( Topology.scouts() ).unique()
ShepardAddr = Topology.datacenters[0].surrogates[0];

// Threads = 4
Duration = 300
InterCmdDelay = 25

AllMachines = ( Topology.allMachines() + ShepardAddr).unique()

Version = getGitCommitId()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

//System.exit(0)

println "==== BUILDING JAR for version " + Version + "..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar", "swiftcloud.jar")
deployTo(AllMachines, "stuff/logging.properties", "logging.properties")

def date = new Date().format('MMMdd-k-m-s-S')

SwiftLinks_Props = "swiftlinks-test.props"

PropsFilenames = args[2..-1]

for (PropsFilename in PropsFilenames) {
    deployTo(AllMachines, PropsFilename, SwiftLinks_Props)

    experimentName = PropsFilename.split('/').last().replace('.props', '')
    
    props = new Properties()
    propIn = new File(PropsFilename).withInputStream { 
      stream -> props.load(stream) 
    }
    lazy = Boolean.valueOf(props.getProperty("swiftlinks.lazy", "false"))
    
    cs = Boolean.valueOf(props.getProperty("swiftlinks.cs", "false"))
    
    println "==== EXPERIMENT '"+experimentName+"'"
        
    if (lazy) {
            println "==== LAZY EXPERIMENT"
    } else {
            println "==== NONLAZY EXPERIMENT"
    }
    
    pssh( Scouts, "rm -Rf swiftlinks-stats")

    def shep = SwiftBase.runShepard( ShepardAddr, Duration, "Released" )

    println "==== LAUNCHING SEQUENCERS"
    Topology.datacenters.each { datacenter ->
        datacenter.deploySequencers(ShepardAddr, "512m")
    }

    Sleep(10)
    println "==== LAUNCHING SURROGATES"
    Topology.datacenters.each { datacenter ->
        datacenter.deploySurrogates(ShepardAddr, "3400m")
    }

    println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
    Sleep(InterCmdDelay)

    println "==== INITIALIZING DATABASE ===="
    def INIT_DB_DC = Topology.datacenters[0].surrogates[0]
    def INIT_DB_CLIENT = Topology.datacenters[0].sequencers[0]

    SwiftLinks.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftLinks_Props, "3000m")
    
    if (cs) {
        println "==== WAITING A BIT BEFORE STARTING CS SERVER ===="
        Sleep(5)
        SwiftLinks.runCSServer(Topology.surrogates(), SwiftLinks_Props, "2048m")
    }

    println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
    Sleep(InterCmdDelay)
    
    if (cs) {
        SwiftLinks.runCSEndClient( Topology.scoutGroups, SwiftLinks_Props, ShepardAddr, Threads, "2048m", lazy )
    } else {
        SwiftLinks.runScouts( Topology.scoutGroups, SwiftLinks_Props, ShepardAddr, Threads, "2048m", lazy )
    }

    println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
    shep.take()

    Countdown( "Max. remaining time: ", Duration + InterCmdDelay + 10)

    pnuke(AllMachines, "java", 60)
    
    def dstDir="results/swiftlinks/"+date+"/"+experimentName+"-"+Version 
    def dstFile = String.format("swiftlinks-DC-%s-SU-%s-CL-%s-TH-%s-%s%s.log", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads, (lazy) ? "lazy" : "nonlazy", (cs) ? "-cs" : "-local")

    pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)
    
    def dstSubDir = String.format("stats-swiftlinks-DC-%s-SU-%s-CL-%s-TH-%s-%s", Topology.datacenters.size(), Topology.datacenters[0].surrogates.size(), Topology.totalScouts(), Threads, (lazy) ? "lazy" : "nonlazy")
    
    pslurp( Scouts, "swiftlinks-stats", dstDir, dstSubDir, 300, false, true)
    
    Sleep(InterCmdDelay)
}

System.exit(0)

