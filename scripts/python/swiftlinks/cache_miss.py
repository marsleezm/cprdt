#!/usr/bin/env python

import os
import sys

import numpy as np
import prettyplotlib as ppl
import matplotlib.pyplot as plt

names = sys.argv[1::2]
dirs = sys.argv[2::2]

starttime = 30000

i = 0



misses = list()

for d in dirs:
	cur_misses = list()
	for (root, subdirs, filenames) in os.walk(d):
		for filename in filenames:
			if filename == 'miss-no-object-count' and 'scout-' in root:
				f = open(root+'/'+filename)
				m = 0
				for line in f:
					(time, elements) = line.split(';')
					time = int(time)
					if time >= starttime:
						elements = int(elements.rstrip())
						m += elements
				cur_misses.append(m)
	misses.append(cur_misses)
	i += 1


i = 0

color1 = ppl.colors.set2[0]
color2 = ppl.colors.set2[1]
ecolor1 = ppl.colors.set1[2]
ecolor2 = ppl.colors.set1[3]

lazyMeans = list()
nonlazyMeans = list()
lazyStd = list()
nonlazyStd = list()

for miss in misses:
	avg = np.mean(miss)
	std_dev = np.std(miss)
	if '-lazy' in dirs[i]:
		lazyMeans.append(avg)
		lazyStd.append(std_dev)
	else:
		nonlazyMeans.append(avg)
		nonlazyStd.append(std_dev)
	print(names[i] + ": " + str(avg) + " " + str(std_dev))
	i += 1

ind = np.arange(len(lazyMeans))  # the x locations for the groups
width = 0.35       # the width of the bars

fig, ax = plt.subplots()
rects1 = ppl.bar(ax, ind, lazyMeans, width, color=color1, grid='y', yerr=lazyStd, align='edge', ecolor=ecolor1)


rects2 = ppl.bar(ax, ind+width, nonlazyMeans, width, color=color2, grid='y', yerr=nonlazyStd, align='edge', ecolor=ecolor2)

# add some
ax.set_ylabel('Number of cache misses')
#ax.set_title('Number of cached elements with and without lazy fetching')
ax.set_xticks(ind+width)
ax.set_xticklabels( names[::2] )
ax.set_xlabel('Cache size limit')

legend = ax.legend( (rects1[0], rects2[0]), ('Lazy', 'Non-lazy') )
legend.draw_frame(False)

def autolabel(rects):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

#autolabel(rects1)
#autolabel(rects2)

plt.xlim([-0.3,len(lazyMeans)])
ymin, ymax = plt.ylim()

plt.ylim((ymin, ymax+30))

#plt.show()
plt.savefig("/tmp/sosp/swiftlinks-cache-misses.pdf")
