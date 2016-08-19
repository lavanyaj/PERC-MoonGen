# Usage: run from MoonGen directory
# TODO: handle errors
#
import os
import subprocess
import sys
import time

import argparse

parser = argparse.ArgumentParser(description='Run Perc')

parser.add_argument('--mode', type=str, help='setup: single for 1:1, multi for 2:2', default='single')
parser.add_argument('--numFlows', type=int, default=1000)
parser.add_argument('--scaling', type=float, default=1.0)
parser.add_argument('--rtt', type=float, default=0.0001)
parser.add_argument('--moongenDir', type=str, default="/home/lavanyaj/MoonGen")
parser.add_argument('--percswitchDir', type=str, default="/home/lavanyaj/perc_switch")

args = parser.parse_args()

print sys.argv

rtt = args.rtt
home = "/home/lavanyaj"
moonGen = "%s/build/MoonGen"%args.moongenDir
mainFile = "%s/examples/perc-moongen-single/main1.lua"%args.moongenDir
mode = args.mode
numFlows = args.numFlows
scaling = args.scaling
cdfFile = "%s/examples/perc-moongen-single/DCTCP_CDF"%args.moongenDir
tmpFile = "%s/out.txt"%args.moongenDir
fctFile = "%s/demo/fct_file.csv"%args.percswitchDir

run_perc = "sudo " + moonGen + " " + mainFile + " " + mode + " " + cdfFile + " " + str(scaling) + " " + str(numFlows)  + " > " + tmpFile
print run_perc

proc = subprocess.Popen(run_perc, stdout=subprocess.PIPE, shell=True)
(out, err) = proc.communicate()


cmd = {}

cmd["medium_tail"] = "grep fct %s | sed 's/ULL//g' | awk '{ if ($10 * 1500 >= 1e4 && $10 * 1500 < 1e6) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/(%f+$2);}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $2;}'" % (tmpFile,rtt)

cmd["medium_median"]= "grep fct %s | sed 's/ULL//g' | awk '{ if ($10 * 1500 >= 1e4 && $10 * 1500 < 1e6) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/(%f+$2);}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $1;}'" % (tmpFile,rtt)

cmd["small_tail"] = "grep fct %s | sed 's/ULL//g' | awk '{ if ($10 * 1500 < 1e4) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/(%f+$2);}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $2;}'" % (tmpFile,rtt)

cmd["small_median"] = "grep fct %s | sed 's/ULL//g' | awk '{ if ($10 * 1500 < 1e4) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/(%f+$2);}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $1;}'" % (tmpFile,rtt)

cmd["large_tail"] = "grep fct %s | sed 's/ULL//g' | awk '{ if ($10 * 1500 >= 1e6) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/(%f+$2);}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $2;}'" % (tmpFile,rtt)

cmd["large_median"] = "grep fct %s | sed 's/ULL//g' | awk '{ if ($10 * 1500 >= 1e6) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/(%f+$2);}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $1;}'" % (tmpFile,rtt)

val = {}
for k in cmd:
    v = cmd[k]
    pipedCommands = v.split("|")
    #print pipedCommands
    proc = subprocess.Popen(v, stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    val[k] = out
    #print k, ": ", out

current_rate = 10000
current_alg = "perc"
fct_small = val["small_tail"]
fct_medium = val["medium_tail"]
fct_large = val["large_median"]
update_id = int(round(time.time()))

f = open(fctFile, "w")
result = (",".join([str(x).rstrip() for x in [current_rate,current_alg,fct_small,fct_medium,fct_large,update_id]]))
print(result)
f.write(result)
f.close()
