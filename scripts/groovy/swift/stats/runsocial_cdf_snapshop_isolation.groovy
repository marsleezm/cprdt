#!/usr/bin/env groovy -classpath .:scripts/groovy:lib/core/ssj.jar
package swift.stats



DIR = new File("/Users/smd/Dropbox/bitbucket-git/swiftcloud-gforce/results/swiftsocial/SOSP/clt_cdfs")
ISOLATION = 'SNAPSHOT_ISOLATION'
new runsocial_latency_cdfs( getBinding() ).run()
