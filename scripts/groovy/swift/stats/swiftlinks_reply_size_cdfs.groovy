#!/bin/bash
//usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar "$0" $@; exit $?
package swift.stats

import static java.lang.System.*
import static swift.stats.GnuPlot.*
import umontreal.iro.lecuyer.stat.Tally

if (args.length < 1) {
    println "usage: script <root_dir_with_logs>"
    System.exit(1)
}

DIR = new File(args[0])

Stats<Integer> stats = new Stats<Integer>();
Set<String> wantedOps = new HashSet<String>();


def processFile = { int L, int H, File f, String placement, String series ->
    err.println f

    long T0 = -1;
    Tally ll = new Tally("size");
    f.eachLine { String line ->
        if (line.startsWith(";") ) {
            err.println line
            return
        }
        String[] tok = line.split(",")
        if (tok.length >= 3) {
            String msg = tok[2]
            if (!msg.equals("METADATA_FetchObjectVersionReply"))
                return
            
            int valueSize = Integer.valueOf(tok[5])
            //int messageSize = Integer.valueOf(tok[4])
            long T = Long.valueOf(tok[1])
            
            T0 = T0 < 0 ? T : T0
            T -= T0
            if (T >= L * 1000 && T <= H * 1000) {
                ll.add(valueSize)
                stats.histogram(placement, series, 1).tally(valueSize, 1.0)
                sizes << String.format("%d", valueSize)
            }
        }
    }
}

def processDir
processDir = { int L, int H, File f, String selector, String topology, String series ->
    if (f.exists()) {
        if (f.isDirectory()) {
            for (File i : f.listFiles()) {
                processDir.call(L, H, i, selector, topology, series);
            }
        } else {
            String name = f.getAbsolutePath();
            if (name.endsWith(".log") && name.contains(selector)) {
                processFile.call(L, H, f, topology, series)
            }
        }
    } else
        err.println("Wrong directory...: " + f);
}

def String search



search = '-lazy'
sizes = []
range = 30
processDir(30, 1000, DIR, search, "*", "lazy")

lazy_series = sizes

search = '-nonlazy'
sizes = []
processDir(30, 1000, DIR, search, "*", "nonlazy")

nonlazy_series = sizes

/*
plots = [
'lazy': lazy_series,
'nonlazy': nonlazy_series
]*/

plots = [:]
for( Histogram i : stats.histograms("*") ) {
    data = []
    int n = i.size()
    Number[] xVal = i.xValues()
    Tally[] yVal = i.yValues()

    double total = 0.0
    yVal.each {
        total += it.sum()
    }

    double accum = 0.0
    n.times  {
        accum += yVal[it].sum()
        data << String.format("%.0f\t%.1f", xVal[it].doubleValue(), 100 * accum / total)
    }
    plots[i.name()] = data
}

def gnuplot = [
    'set encoding iso_8859_1',
    'set terminal postscript size 10.0, 6.0 enhanced font "Helvetica,24"',
    //                   'set terminal aqua',
    'set xlabel "Message payload size [ bytes ]"',
    'set ylabel "Cumulative Ocurrences [ % ]"',
    'set style line 1',
    'set mxtics',
    'set mytics',
    'set logscale x',
    'set yr [0:100]',
    'set pointinterval 500',
    'set key right bottom',
    'set grid xtics ytics lt 30 lt 30'
]

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}

GnuPlot.doGraph( '/tmp/sosp/SwiftLinks-reply-size-box', gnuplot, plots, { k, v ->
    String.format(' with lines title "%s"', k)
}, keySorter )
