# Usage: run from MoonGen directory
# TODO: handle errors
#
import os
import subprocess

run_perc = "sudo ./build/MoonGen examples/perc-moongen-single/main.lua 0 1 examples/perc-moongen/DCTCP_CDF 1000 > out.txt"
proc = subprocess.Popen(run_perc, stdout=subprocess.PIPE, shell=True)
(out, err) = proc.communicate()


cmd = {}

cmd["medium_tail"] = "grep fct out.txt | sed 's/ULL//g' | awk '{ if ($10 * 1500 >= 1e4 && $10 * 1500 < 1e6) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/$2;}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $2;}'"

cmd["medium_median"]= "grep fct out.txt | sed 's/ULL//g' | awk '{ if ($10 * 1500 >= 1e4 && $10 * 1500 < 1e6) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/$2;}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $1;}'"

cmd["small_tail"] = "grep fct out.txt | sed 's/ULL//g' | awk '{ if ($10 * 1500 < 1e4) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/$2;}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $2;}'"

cmd["small_median"] = "grep fct out.txt | sed 's/ULL//g' | awk '{ if ($10 * 1500 < 1e4) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/$2;}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $1;}'"

cmd["large_tail"] = "grep fct out.txt | sed 's/ULL//g' | awk '{ if ($10 * 1500 >= 1e6) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/$2;}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $2;}'"

cmd["large_median"] = "grep fct out.txt | sed 's/ULL//g' | awk '{ if ($10 * 1500 >= 1e6) {print $8*1e6 \" \" $10*1.2;}}' | awk '{print $1/$2;}' | Rscript -e 'quantile (as.numeric (readLines (\"stdin\")), probs=c(0.5, 0.95))' | tail -n 1 | awk '{print $1;}'"

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
current_alg = "PERC"
fct_small = val["small_tail"]
fct_medium = val["medium_tail"]
fct_large = val["large_median"]
f = open("fct_file.csv", "w")
result = (",".join([str(x).rstrip() for x in [current_rate,current_alg,fct_small,fct_medium,fct_large]]))
print(result)
f.write(result)
f.close()
