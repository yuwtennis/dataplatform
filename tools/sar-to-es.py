#!/usr/bin/python

def parseArgs():
    import argparse

    parser = argparse.ArgumentParser( description='This program will parse lines from sadf command and create elasticsearch index.')
    parser.add_argument('-q', '--queue',   help='Equivalent to sar -q option', action='store_true')
    parser.add_argument('-d', '--device',  help='Equivalent to sar -d option', action='store_true')
    parser.add_argument('-n', '--network', help='Equivalent to sar -n DEV option', action='store_true')
    parser.add_argument('-b', '--blocks',  help='Equivalent to sar -b option', action='store_true')
    parser.add_argument('-H', '--Host',    help='Elasticsearch host', nargs='?', default='localhost:9200')
    parser.add_argument('--directory',     type=str, help='Directory where sa object file exists')
    parser.add_argument('--sa_file',       type=str, help='Sar object file name')

    return parser.parse_args()

def parseSADF(filePath, opt):
    import subprocess
    from subprocess import Popen,PIPE

    # Execute sadf command
    process = subprocess.Popen('/usr/bin/sadf -t -d {0} -- {1}'.format( filePath, opt),stdout=PIPE, shell=True)

    # Store all lines into list.
    # sadf command returns first line as header.
    # Also when OS restart occurs it records something like
    # dev1.ywlocal.net;-1;2018-11-08 14:26:17;LINUX-RESTART
    # Make sure to exclude these lines.
    lines = [x.strip() for x in process.stdout.readlines() if not "# hostname" in x and not "LINUX-RESTART" in x]

    return lines

def main():
    from devEs import devEs
    from elasticsearch import Elasticsearch
    import logging
    import sys

    logging.basicConfig(level=logging.INFO)

    # Parse input arguments
    logging.info("Parsing all the arguments.")
    args = parseArgs()

    if not args.directory or not args.sa_file: 
        logging.error( "Directory or sa file not specified!" )
        sys.exit()

    # Create elasticsearch connection instance
    logging.info("Connecting to elasticsearch. Host: {}".format(args.Host))

    es = Elasticsearch(args.Host)
    if not es.ping():
        logging.error("Failed to connect to elasticsearch instance.")
        sys.exit()

    logging.info("Connection success.")

    # Files must be parsed using sadf -d -t [SAR_FILE] -- sar options
    o = devEs()
    if args.queue:
        # hostname;interval;timestamp;runq-sz;plist-sz;ldavg-1;ldavg-5;ldavg-15;blocked
        # SAR -q
        CSV_COLUMNS="hostname;interval;timestamp;runq-sz;plist-sz;ldavg-1;ldavg-5;ldavg-15;blocked"
        ES_INDEX_PREFIX="sar-q"
        MAPPING=o.prepareSarQueueMapping()
        OPT="-q"

    elif args.device:
        # hostname;interval;timestamp;DEV;tps;rd_sec/s;wr_sec/s;avgrq-sz;avgqu-sz;await;svctm;%util
        # sar -d
        CSV_COLUMNS="hostname;interval;timestamp;dev;tps;rd-sec-per-sec;wr-sec-per-sec;avgrq-sz;avgqu-sz;await;svctm;percent-util"
        ES_INDEX_PREFIX="sar-d"
        MAPPING=o.prepareSarDeviceMapping()
        OPT="-d"

    elif args.network:
        # hostname;interval;timestamp;IFACE;rxpck/s;txpck/s;rxkB/s;txkB/s;rxcmp/s;txcmp/s;rxmcst/s
        # sar -n DEV
        CSV_COLUMNS="hostname;interval;timestamp;iface;rxpck_per_sec;txpck_per_sec;rxkb_per_sec;txkB_per_sec;rxcmp_per_sec;txcmp_per_sec;rxmcst_per_sec"
        ES_INDEX_PREFIX="sar-n"
        MAPPING=o.prepareSarNetworkMapping()
        OPT="-n DEV"

    elif args.blocks:
        # hostname;interval;timestamp;tps;rtps;wtps;bread/s;bwrtn/s
        # sar -b
        CSV_COLUMNS="hostname;interval;timestamp;tps;rtps;wtps;bread_per_sec;bwrtn_per_sec"
        ES_INDEX_PREFIX="sar-b"
        MAPPING=o.prepareSarBlocksMapping()
        OPT="-b"

    else:
        print "No option specified!"
        sys.exit()

    # Get sadf results
    logging.info("Parsing sar file from directory: {0}, file: {1}".format( args.directory, args.sa_file ))
    lines = parseSADF("{0}/{1}".format(args.directory, args.sa_file), OPT)

    logging.debug(lines)

    # Prepare index
    logging.info("Preparing elasticsearch index.")

    # Parse date from first line of the sadf result
    host = (lines[0].split(';'))[0].lower()
    date = (lines[0].split(';'))[2].split(' ')[0].replace('-', '.')
    esIndex="{0}-{1}-{2}".format(ES_INDEX_PREFIX, host, date)

    if not es.indices.exists(esIndex):
        es.indices.create(esIndex,body=MAPPING)

    # Load Data to elasticsearch
    o.sendBulk( es, esIndex, CSV_COLUMNS , lines)

if __name__ == "__main__":
   main()
