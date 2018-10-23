#!/bin/bash

FILEBEAT_LOG_DIR=$1
TMP_FILE1=OPEN_CLOSE.tmp
TMP_FILE2=LOG.tmp

if [[ ! -d $FILEBEAT_LOG_DIR ]]
then
    echo "Directory: $FILEBEAT_LOG_DIR does not exist!"
    exit
fi

echo "Remove temporary file"
rm -v $TMP_FILE1 $TMP_FILE2

echo "Find all harvestor related logs from filebeat"
sudo chown -R ywatanabe:ywatanabe $FILEBEAT_LOG_DIR
egrep -h "Harvester started for file|INFO File" ${FILEBEAT_LOG_DIR}/filebeat* | sort -k 1 > $TMP_FILE1

echo "Parse all filebeat files..."
./filebeatlog.pl $TMP_FILE1 &> $TMP_FILE2

echo "Load to bigquery"
./load_fb_csv_to_bq.py $TMP_FILE2

echo "Done!"
