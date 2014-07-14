package swift.deployment

import static swift.deployment.Tools.*
import static swift.deployment.SwiftBase.*

class Topology {
    static List datacenters = []
    static List scoutGroups = []

    static String dcKey( int index ) {
        return "" + ('XYZABCDEFGHIJKLMNOPQRSTUVWxyzabcdefghijklmnopqrstuvw').charAt(index);
    }

    def static Datacenter DC(List sequencers, List surrogates) {
        return new Datacenter( sequencers, surrogates)
    }

    def static ScoutGroup SGroup(List clients, Datacenter dc) {
        return new ScoutGroup( clients, dc)
    }

    def static List allMachines() {
        def res = []
        datacenters.each {
            res += it.sequencers
            res += it.surrogates
        }
        res += scouts()
        res = res.unique()
    }

    def static List scouts() {
        def res = []
        scoutGroups.each{
            res += it.all()
        }
        return res
    }

    def static int totalScouts() {
        int res = 0;
        scoutGroups.each{
            res += it.all().size()
        }
        return res
    }

    static class Datacenter {
        def List sequencers
        def List surrogates

        def public Datacenter(List sequencers, List surrogates) {
            this.sequencers = sequencers.unique()
            this.surrogates = surrogates.unique()
            Topology.datacenters += this;
        }

        def List all() {
            return (sequencers + surrogates).unique()
        }


        static def List sequencers() {
            def res = [];
            Topology.datacenters.each { dc ->
                res += dc.sequencers
            }
            return res.unique()
        }

        void deploySequencersExtraArgs(String shepard, String extraArgs, String seqHeap = "256m") {
            def otherSequencers = sequencers() - this.sequencers
            def siteId = dcKey( Topology.datacenters.indexOf(this));

            sequencers.each { host ->
                rshC(host, swift_app_cmd( "-Xms"+seqHeap+" -Xmx"+seqHeap, sequencerCmd(siteId, shepard, surrogates, otherSequencers, extraArgs), "seq-stdout.txt", "seq-stderr.txt" ))
            }
        }

        void deploySequencers(String shepard, String seqHeap = "256m") {
            deploySequencersExtraArgs( shepard, "", seqHeap)
        }

        void deploySurrogatesExtraArgs(String shepard, String extraArgs, String surHeap = "512m") {
            def siteId = dcKey( Topology.datacenters.indexOf(this));

            surrogates.each { host ->
                def otherSurrogates = surrogates - host
                rshC(host, swift_app_cmd( "-Xms"+surHeap+" -Xmx"+surHeap, surrogateCmd( siteId, shepard, sequencers[0], otherSurrogates, extraArgs ), "sur-stdout.txt", "sur-stderr.txt" ))
            }
        }

        void deploySurrogates(String shepard, String surHeap = "512m") {
            deploySurrogatesExtraArgs( shepard, "", surHeap)
        }
    }


    static class ScoutGroup {
        List scouts
        Datacenter dc

        ScoutGroup( List scouts, Datacenter dc) {
            this.scouts = scouts
            this.dc = dc
            Topology.scoutGroups += this
        }

        void deploy( Closure cmd, Closure resHandler) {
            Parallel.rsh( scouts, cmd, resHandler, false, 500000)
        }


        def List all() {
            return scouts
        }
    }
}
