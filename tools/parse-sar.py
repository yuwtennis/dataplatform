#!/usr/bin/python

from devEs import devEs
from elasticsearch import Elasticsearch

def main():
    import sys

    # Files must be parsed using sadf -d -t [SAR_FILE] -- sar options
    # ex. # hostname;interval;timestamp;runq-sz;plist-sz;ldavg-1;ldavg-5;ldavg-15;blocked
    #CSV_COLUMNS="hostname;interval;timestamp;runq-sz;plist-sz;ldavg-1;ldavg-5;ldavg-15;blocked"
    CSV_COLUMNS="hostname;interval;timestamp;dev;tps;rd-sec-per-sec;wr-sec-per-sec;avgrq-sz;avgqu-sz;await;svctm;percent-util"
    ES_HOST="localhost:9200"
    ES_INDEX="sar-d"

    file = sys.argv[1]

    with open( file, "r") as fd:
        lines = fd.readlines() 

    lines = [x.strip() for x in lines]

    # Remove first line since its header
    del lines[0]
 
    es = Elasticsearch(ES_HOST)
    o = devEs()

    o.setIndex( es, ES_INDEX, o.prepareSarQMapping() )
    o.sendBulk( es, ES_INDEX, CSV_COLUMNS , lines)


if __name__ == "__main__":
   main()
