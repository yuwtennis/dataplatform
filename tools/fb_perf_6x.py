#!/usr/bin/python
# coding=utf-8

import re
import json
import sys
import ast

dir  = sys.argv[1]
file = sys.argv[2]

re1 = re.compile("^(\S+)\s+.*	({.*})$")
print "{0},{1},{2}".format("file", "timestamp", "acked" )

with open( dir + "/" + file ) as fd:
    for line in fd:
        m = re1.search( line )
        if m:
             (a , b) = m.groups()

             d =  json.loads(b)

             if 'monitoring' in d:

                 c3 = 0

                 c1 = d['monitoring']['metrics']['filebeat']['harvester']['open_files']
                 c2 = d['monitoring']['metrics']['filebeat']['harvester']['running']

                 if 'events' in  d['monitoring']['metrics']['libbeat']['output']:
                     c3 = d['monitoring']['metrics']['libbeat']['output']['events']['acked']

                 c4 =  d['monitoring']['metrics']['registrar']['states']['current']
                 c5 =  d['monitoring']['metrics']['registrar']['states']['update']
                 c6 =  d['monitoring']['metrics']['registrar']['writes']['total']
                 c7 =  d['monitoring']['metrics']['registrar']['writes']['success']

                 print "{0},{1},{2}".format(file, a, c3)
