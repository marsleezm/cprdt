#!/usr/bin/env groovy -classpath .:scripts/groovy:scripts/groovy/lib

import static Tools.*
import static PlanetLab_3X.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})
Surrogates = [
    "ec2-54-228-106-66.eu-west-1.compute.amazonaws.com",
//    "ec2-50-112-200-169.us-west-2.compute.amazonaws.com",
//    "ec2-54-242-185-216.compute-1.amazonaws.com",
//    "ec2-54-249-137-48.ap-northeast-1.compute.amazonaws.com",
//    "ec2-54-228-109-231.eu-west-1.compute.amazonaws.com",
//    "ec2-54-241-202-13.us-west-1.compute.amazonaws.com",
//    "ec2-54-244-182-93.us-west-2.compute.amazonaws.com"
]

Scouts = (PlanetLab_NC + PlanetLab_NV + PlanetLab_EU).unique()

Shepard = Surrogates.get(0);

def Threads = 3
def Duration = 180
def SwiftSocial_Props = "swiftsocial-test.props"


AllMachines = (Surrogates + Scouts + Shepard).unique()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

//System.exit(0)

//println "==== BUILDING JAR..."
//sh("ant -buildfile smd-jar-build.xml").waitFor()
//deployTo(AllMachines, "swiftcloud.jar")
//deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")
deployTo(AllMachines, SwiftSocial_Props)


def shep = SwiftSocial.runShepard( Shepard, Duration, "Released" )

SwiftSocial.runEachAsDatacentre(Surrogates, "256m", "3096m")

println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(5)


//println "==== INITIALIZING DATABASE ===="
//def INIT_DB_DC = Surrogates.get(0)
//def INIT_DB_CLIENT = Surrogates.get(0)
//
//SwiftSocial.initDB( INIT_DB_CLIENT, INIT_DB_DC, SwiftSocial_Props)


println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(10)


SwiftSocial.runStandaloneScouts( Scouts, Surrogates, SwiftSocial_Props, Shepard, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/swiftsocial/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("1pc-results-swiftsocial-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec(["/bin/bash", "-c", "wc " + dstDir + "/*/*"]).waitFor()
System.exit(0)

