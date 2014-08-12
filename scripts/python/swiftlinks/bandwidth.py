#!/usr/bin/env python

import os
import sys
import re

import numpy as np
import prettyplotlib as ppl
import matplotlib.pyplot as plt

dirs = sys.argv[1:]

i = 0

sizes = list()

duration = 30000

duration_after = 60000

sizes_after = list()

for d in dirs:
	cur_sizes = list()
	cur_sizes_after = list()
	for (root, subdirs, filenames) in os.walk(d):
		for filename in filenames:
			if filename.endswith(".log"):
				g = re.search("TH-(\d+)", filename)
				threads = int(g.group(1))
				f = open(root+'/'+filename)
				size = 0
				sizeafter = 0
				maxtime = 0
				maxtime_after = 0
				for line in f:
					if not line.startswith(';'):
						fields = line.split(',')
						if fields[1] == 'INIT' and maxtime == 0:
							maxtime = int(fields[3]) + duration
							maxtime_after = maxtime + duration_after
						if fields[2] == 'METADATA_FetchObjectVersionReply':
							if int(fields[1]) < maxtime:
								size += int(fields[3])
							else:
								if int(fields[1]) < maxtime_after:
									sizeafter += int(fields[3])
				
				size = float(size) / threads
				sizeafter = float(sizeafter) / threads
				
				cur_sizes.append(size)
				cur_sizes_after.append(sizeafter)
	sizes.append(cur_sizes)
	sizes_after.append(cur_sizes_after)
	i += 1

color1 = ppl.colors.set2[0]
color2 = ppl.colors.set2[1]
ecolor1 = ppl.colors.set1[2]
ecolor2 = ppl.colors.set1[3]

i = 0
for s in sizes:
	s = list(map(lambda x: float(x)/(duration), s))
	avg = np.mean(s)
	std_dev = np.std(s)
	if '-nonlazy' in dirs[i]:
		nonlazyMean = avg
		nonlazyStd = std_dev
	else:
		lazyMean = avg
		lazyStd = std_dev
	print(str(avg) + " " + str(std_dev))
	i += 1


#ind = np.arange(2)  # the x locations for the groups
width = 0.5       # the width of the bars

fig, ax = plt.subplots()
rects1 = ppl.bar(ax, [0], [lazyMean], width, color=color1, grid='y', annotate=True, yerr=[lazyStd], align='center', ecolor=ecolor1)


rects2 = ppl.bar(ax, [1], [nonlazyMean], width, color=color2, grid='y', annotate=True, yerr=[nonlazyStd], align='center', ecolor=ecolor2)

# add some
ax.set_ylabel('Bandwidth (KB/s)')
#ax.set_title('Bandwidth usage during the first 30 seconds')
ax.set_xticks([0, 1])
ax.set_xticklabels( ('Lazy', 'Non-lazy' ) )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

#autolabel(rects1)
#autolabel(rects2)

plt.xlim([-0.5,1.5])
#ymin, ymax = plt.ylim()

plt.ylim((0, 250))

#plt.show()
plt.savefig("/tmp/sosp/swiftlinks-bandwidth-startup-"+str(duration)+".pdf")

i = 0
for s in sizes_after:
	s = list(map(lambda x: float(x)/(duration), s))
	avg = np.mean(s)
	std_dev = np.std(s)
	if '-nonlazy' in dirs[i]:
		nonlazyMean = avg
		nonlazyStd = std_dev
	else:
		lazyMean = avg
		lazyStd = std_dev
	print(str(avg) + " " + str(std_dev))
	i += 1


#ind = np.arange(2)  # the x locations for the groups
width = 0.5       # the width of the bars

fig, ax = plt.subplots()
rects1 = ppl.bar(ax, [0], [lazyMean], width, color=color1, grid='y', yerr=[lazyStd], annotate=True, align='center', ecolor=ecolor1)


rects2 = ppl.bar(ax, [1], [nonlazyMean], width, color=color2, grid='y', yerr=[nonlazyStd], annotate=True, align='center', ecolor=ecolor2)

# add some
ax.set_ylabel('Bandwidth (KB/s)')
#ax.set_title('Bandwidth usage during the first 30 seconds')
ax.set_xticks([0, 1])
ax.set_xticklabels( ('Lazy', 'Non-lazy' ) )

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

#autolabel(rects1)
#autolabel(rects2)

plt.xlim([-0.5,1.5])
#ymin, ymax = plt.ylim()

plt.ylim((0, 220))

#plt.show()
plt.savefig("/tmp/sosp/swiftlinks-bandwidth-after-"+str(duration)+"-"+str(duration_after) +".pdf")
