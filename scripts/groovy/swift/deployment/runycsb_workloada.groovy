#!/usr/bin/env groovy -classpath .:scripts/groovy
package swift.deployment

import static swift.deployment.SwiftYCSB.*
import static swift.deployment.Tools.*

def __ = onControlC({
    pnuke(AllMachines, "java", 60)
    System.exit(0);
})


Surrogates = [
    'ec2-54-73-46-174.eu-west-1.compute.amazonaws.com'
    //    'ec2-54-228-122-250.eu-west-1.compute.amazonaws.com',
    //    'ec2-54-244-165-15.us-west-2.compute.amazonaws.com',
    //    'ec2-54-234-203-38.compute-1.amazonaws.com'
]


//    Scouts = (PlanetLab_EU).unique()
// (PlanetLab_NC + PlanetLab_NV + PlanetLab_EU).unique()
Scouts = [
    'ec2-54-73-82-130.eu-west-1.compute.amazonaws.com',
    'ec2-54-220-48-153.eu-west-1.compute.amazonaws.com',
    'ec2-176-34-95-49.eu-west-1.compute.amazonaws.com',
    'ec2-46-137-3-177.eu-west-1.compute.amazonaws.com',
    'ec2-54-195-134-197.eu-west-1.compute.amazonaws.com',
    'ec2-54-73-32-242.eu-west-1.compute.amazonaws.com',
    'ec2-46-137-5-9.eu-west-1.compute.amazonaws.com',
    'ec2-54-73-31-206.eu-west-1.compute.amazonaws.com',
    'ec2-54-73-110-241.eu-west-1.compute.amazonaws.com',
    'ec2-54-73-132-28.eu-west-1.compute.amazonaws.com'
]

Threads = 1
YCSBProps = "swiftycsb.properties"

// TODO use Shepard and increase Duration?
Duration = 320

AllMachines = (Surrogates + Scouts).unique()

println getBinding().getVariables()

dumpTo(AllMachines, "/tmp/nodes.txt")

pnuke(AllMachines, "java", 60)

println "==== BUILDING JAR..."
sh("ant -buildfile smd-jar-build.xml").waitFor()
deployTo(AllMachines, "swiftcloud.jar")
deployTo(AllMachines, "stuff/all_logging.properties", "all_logging.properties")
deployTo(AllMachines, SwiftYCSB.genPropsFile(['recordcount':'10000',
    'operationcount':'20000', 'fieldlength':'1', 'fieldcount':'1'], SwiftYCSB.DEFAULT_PROPS + SwiftYCSB.WORKLOAD_A).absolutePath, YCSBProps)


SwiftYCSB.runEachAsDatacentre(Surrogates, "256m", "3096m")
println "==== WAITING A BIT BEFORE INITIALIZING DB ===="
Sleep(10)

println "==== INITIALIZING DATABASE ===="
def INIT_DB_DC = Surrogates.get(0)
def INIT_DB_CLIENT = Surrogates.get(0)

SwiftYCSB.initDB( INIT_DB_CLIENT, INIT_DB_DC, YCSBProps, Threads)


println "==== WAITING A BIT BEFORE STARTING SCOUTS ===="
Sleep(10)
// TODO client-DC assignment
SwiftYCSB.runClients(Scouts, Surrogates.get(0), YCSBProps, Threads )

println "==== WAITING FOR SHEPARD SIGNAL PRIOR TO COUNTDOWN ===="
// TODO
// shep.take()

Countdown( "Remaining: ", Duration + 30)

pnuke(AllMachines, "java", 60)

def dstDir="results/ycsb/workloada/" + new Date().format('MMMdd-') + System.currentTimeMillis()
def dstFile = String.format("results-ycsb-DC-%s-SC-%s-TH-%s.log", Surrogates.size(), Scouts.size(), Threads)

pslurp( Scouts, "scout-stdout.txt", dstDir, dstFile, 300)

exec([
    "/bin/bash",
    "-c",
    "wc " + dstDir + "/*/*"
]).waitFor()

System.exit(0)
