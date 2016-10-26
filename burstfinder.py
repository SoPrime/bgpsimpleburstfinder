#!/usr/bin/env python


# Test Application for pybgpstream.
# finds bursts in bgp.


#from __future__ import braces
import sys
from _pybgpstream import BGPStream, BGPRecord, BGPElem
from math import floor


def count_valid_withdrawals_per_second(t0,num_seconds):
    # div0 in progress tracking if <100 seconds
    assert num_seconds>100;

    # Create a new bgpstream instance
    stream = BGPStream();
    # use Routeviews Saopaulo records
    stream.add_filter('project','routeviews');
    stream.add_filter('collector','route-views.saopaulo');
    stream.add_filter('record-type','updates');
    # time interval
    stream.add_interval_filter(t0,t0+tt-1);

    # progress tracking
    prog = t0;    

    # Create a reusable bgprecord instance
    rec = BGPRecord();
    # list of zeroes
    ctr = [0]*tt;
    # Start the stream
    stream.start();

    # iterate over records
    while(stream.get_next_record(rec)):
        if rec.status == "valid":
            elem = rec.get_next_elem();
            while(elem):
                if elem.type == 'W':
                    ctr[rec.time-t0] += 1;
                elem = rec.get_next_elem();
        # el-cheapo progress indication with dots. comment out if you don't want the dots.
        # """
        if rec.time > prog and (rec.time - t0) % floor(num_seconds/100) == 0:
            sys.stdout.write('.');
            sys.stdout.flush();
            prog = rec.time;
        # """
    # print newline after the dots
    print "";
    return ctr;


# int main() {

# start time
t0=1476728093;
# number of seconds to check in total
tt=86400;
# sliding window interval length (seconds)
dt=10;
# burst threshold
th=1500;

# flags for tracking bursts
exist_burst = False;
burst_running = False;

# running burst wcount
running_cnt = 0;
# running burst start time
burst_starttime = 0;


# get count array
wcount = count_valid_withdrawals_per_second(t0,tt);


# loop over array in dt-intervals
# and get sliding window withdrawal count
# to detect bursts.
for i in range(dt, tt):
    # sum up withdrawals in current window
    ccnt = sum(wcount[i-dt:i]);
    if ccnt > th and not burst_running:
        # new burst found
        burst_starttime = t0+i-dt;
        exist_burst = True;
        burst_running = True;
        running_cnt += wcount[i-dt];
    elif ccnt > th and burst_running:
        # burst ongoing
        running_cnt += wcount[i-dt];
    elif not (ccnt > th) and burst_running:
        # burst finished, print information
        burst_duration = t0+i-dt-burst_starttime;
        print "burst detected: from", burst_starttime, "to", t0+i-dt, "-", running_cnt,"withdrawals in", burst_duration,"seconds, on average", running_cnt/burst_duration, "withdrawals per second.";
        burst_running = False;
        running_cnt = 0;

# output information if no bursts were found.
if not exist_burst:
    print "no bursts found in interval ",t0," to ",t0+tt;

# }
