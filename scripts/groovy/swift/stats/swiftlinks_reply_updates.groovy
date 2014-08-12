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

            int numberOfUpdates = Integer.valueOf(tok[7])
            long T = Long.valueOf(tok[1])
            
            T0 = T0 < 0 ? T : T0
            T -= T0
            if (T >= L * 1000 && T <= H * 1000) {
                int index = ((int)(T / (range*1000))) * range
                currentVals = series_per_time_range[index]
                if (currentVals == null) {
                    series_per_time_range[index] = []
                }
                series_per_time_range[index] << numberOfUpdates
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
series_per_time_range = [:]
range = 30
processDir(30, 1000, DIR, search, "*", "lazy")

lazy_series = series_per_time_range

search = '-nonlazy'
series_per_time_range = [:]
processDir(30, 1000, DIR, search, "*", "nonlazy")

nonlazy_series = series_per_time_range

lazy_series_sum = []
lazy_series.each { key, value ->
        lazy_series_sum << String.format("%d %d", key, value.sum())
    }
nonlazy_series_sum = []
nonlazy_series.each { key, value ->
        nonlazy_series_sum << String.format("%d %d", key, value.sum())
    }

lazy_series_avg = []
lazy_series.each { key, value ->
        lazy_series_avg << String.format("%d %.3f", key, value.sum()/(double)value.size())
    }
nonlazy_series_avg = []
nonlazy_series.each { key, value ->
        nonlazy_series_avg << String.format("%d %.3f", key, value.sum()/(double)value.size())
    }

plots = [
'lazy sum': lazy_series_sum,
'nonlazy sum': nonlazy_series_sum
]

plots_avg = [
'lazy avg': lazy_series_avg,
'nonlazy avg': nonlazy_series_avg
]

def gnuplot = [
    'set encoding iso_8859_1',
    'set terminal postscript size 10.0, 6.0 enhanced monochrome dashed font "Helvetica,24" linewidth 1',
    //                   'set terminal aqua dashed',
    'set xlabel "Time [ s ]"',
    'set ylabel "Sum of CRDT state size in messages [ % ]"',
    'set mxtics',
    'set mytics',
    'set pointinterval 20',
    'set key right bottom',
    'set grid xtics ytics lt 30 lt 30',
    'set label',
    'set clip points',
    'set lmargin at screen 0.1',
    'set rmargin at screen 0.99',
    'set bmargin at screen 0.0',
    'set tmargin at screen 0.9999',
]

//Sort by key length, then alphabetically
def keySorter = { String a, b ->
    int l = Integer.signum( a.length() - b.length() )
    l == 0 ? a.compareTo(b) : l
}

GnuPlot.doGraph( '/tmp/sosp/SwiftLinks-reply-updates-size-sum', gnuplot, plots, { k, v ->
    String.format('title "%s" with linespoints pointinterval 4 lw 3 ps 2', k)
}, keySorter )

GnuPlot.doGraph( '/tmp/sosp/SwiftLinks-reply-updates-size-avg', gnuplot, plots_avg, { k, v ->
    String.format('title "%s" with linespoints pointinterval 4 lw 3 ps 2', k)
}, keySorter )
