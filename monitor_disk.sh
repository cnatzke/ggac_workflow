#!/bin/bash
set -e
#DIRS="/nfs/ccg3/scratch-purge-no-backups/workflow.isi.edu/scratch3/rynge/irri/scratch /nfs/ccg4/scratch-purge-no-backups/workflow.isi.edu/scratch2/rynge/irri/scratch"
DIRS="/home/cnatzke/workflows/scratch/cnatzke/pegasus/ggac_surface-workflow"
OUTPUT_DIR="/public/cnatzke"

for DIR in $DIRS; do
    cd $DIR
    for SUBDIR in `ls`; do
        cd $OUTPUT_DIR
        if [ ! -e $SUBDIR.rrd ]; then
            rrdtool create $SUBDIR.rrd \
                    --step 900 \
                    DS:diskusage:GAUGE:7200:0:U \
                    RRA:AVERAGE:0.5:1:8760 \
                    RRA:MIN:0.5:24:8760 \
                    RRA:MAX:0.5:24:8760
        fi
        # now collect the disk usage and insert it into the RRD
        cd $DIR
        DU=`nice du -s $SUBDIR | awk '{print $1;}'`
        # du is in kb, correct it to bytes
        DU=$(($DU * 1024))
        cd $OUTPUT_DIR
        CMD="rrdtool update $SUBDIR.rrd N:$DU"
        echo $CMD
        $CMD
        # some default graphs
        #for DT in 1day 2day 3day 1week 1month; do
        for DT in 1day 2day; do
            rrdtool graph $SUBDIR-$DT.png \
                    --start -$DT \
                    --width 600 --height 400 \
                    --lower-limit 0 \
                    --slope-mode \
                    --vertical-label "Bytes" \
                    DEF:diskusage=$SUBDIR.rrd:diskusage:AVERAGE \
                    LINE2:diskusage#0000FF:"Disk Usage\l"
        done
    done
done
