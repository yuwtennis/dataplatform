#!/usr/bin/python

import re
import json
import sys

dir  = sys.argv[1]
file = sys.argv[2]

re1 = re.compile("^(\S+)\s+.*?libbeat.logstash.published_and_acked_events=([0-9]+).*?registrar.states.update=([0-9]+) registrar.writes=([0-9]+).*?$")

print "file,timestamp,acked"

with open( dir + "/" + file ) as fd:
    for line in fd:
        m = re1.search( line )
        if m:
             (a, b, c, d) = m.groups()

             print "{0},{1},{2}".format(file, a, b)
