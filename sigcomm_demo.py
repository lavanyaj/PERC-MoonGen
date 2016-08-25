#!/usr/bin/env python

#import Tkinter
#import matplotlib
#matplotlib.use('TKAgg')

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Slider, Button, RadioButtons
from matplotlib import animation

import random, time, csv, sys, subprocess

class GUI:
    """
    This is the GUI for the PERC Switch SIGCOMM 2016 demo
    """
    def __init__(self, fig, ax): 
        self.fig = fig
        self.ax = ax
        self.fig.subplots_adjust(left=0.2, bottom=0.25)
        self.axcolor = 'lightgoldenrodyellow'
        
        self.ax.set_xlabel('Flow Size', fontweight="bold")
        self.ax.set_ylabel('FCT Normalized by Ideal FCT', fontweight="bold")
        self.ax.set_title('PERC -- Flow Completion Time', fontweight="bold")        
        self.ax.grid()

        """
        Expected format of the fct_file.csv:
        <current_rate>,<current_alg>,<FCT for small flows>,<FCT for medium flows>,<FCT for large flows>,<update_ID>
        """
        self.moongenDir = '/home/sibanez/tools/MoonGen'
        self.results_file = 'fct_file.csv'
        self.perc_script_file = self.moongenDir + '/examples/perc-moongen-single/run_perc.py' 
        self.insert_DPDK_module_script = self.moongenDir + '/bind-interfaces.sh'
        self.NIC_driver_script = self.moongenDir + '/deps/dpdk/tools/dpdk_nic_bind.py'
        self.NIC_pcie_slots = ['0000:04:00.0', '0000:04:00.1', '0000:23:00.0', '0000:23:00.1']
        self.percswitchDir = '/home/sibanez/projects/perc_switch'

        self.interfaces = ['eth5', 'eth6', 'eth7', 'eth8']
        self.ip_addresses = ['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']
        self.tcp_benchmarkDir = '/home/sibanez/tools/tcp-benchmark'
        self.num_tcp_senders = 2
        self.setup_hugetables_script = self.moongenDir + '/setup-hugetlbfs.sh'

        self.YMAX = 10
        # Set up the plot with empty data
        self.numGroups = 3 
        self.index = np.arange(self.numGroups)
        self.bar_width = 0.40
        self.alpha = 0.5
        self.stale_alpha = 0.1

        # This adjust the scale of the flows when we run them to simualte running
        #   at different link speeds
        self.flowScale = 100
       
        # run the PERC algorithm and get the results
        self.setup_PERC()
        self.program_fpga()
        self.perc_fct_current = [5, 10, 15]
        self.perc_fct_current = self.run_PERC(self.flowScale)      
        print "##########################"
        print "Initial results = ", self.perc_fct_current
        print "##########################"

        self.perc_color = 'g'
        self.perc_rects = self.ax.bar(self.index + self.bar_width/2, self.perc_fct_current, self.bar_width,
                             alpha=self.alpha, color=self.perc_color, label='PERC')

        plt.xticks(self.index + self.bar_width, ('small flows', 'medium flows', 'large flows'))

        self.ax.set_ylim(0, self.YMAX)

#        self.create_OrigFlowsButton()
#        self.create_ShortFlowsButton()
#        self.ButtonOF.color = 'green'
#        self.last_time = time.time()
#        self.alg_swap_interval = 20.0
#        self.current_alg = 'perc'
#        self.updateID = -1

    def init_plot(self):
        return (self.perc_rects)

    def data_gen(self):
        """
        This function is called over and over to generate the data for the plot
        """
        while True:
            self.program_fpga()
            new_fct_vals = self.run_PERC(self.flowScale)
            new_fct_vals = self.perc_fct_current
            yield new_fct_vals
                
    def run(self, data):
        """
        This function receives as input the data from data_gen
        """
        # The file has not been updated since the last iteration
        new_fct_vals = data
        self.updatePERC_data(new_fct_vals)
        return (self.perc_rects)

    """
    Updates the PERC FCT bars and the instantaneous line
    """
    def updatePERC_data(self, new_fct_vals):
        for (perc_height, perc_rect) in zip(new_fct_vals, self.perc_rects):
            perc_rect.set_height(perc_height)

    def create_OrigFlowsButton(self):
        axOFButton = plt.axes([0.3, 0.1, 0.2, 0.05], axisbg=self.axcolor)
        self.ButtonOF = Button(axOFButton, 'Original Flows', color=self.axcolor, hovercolor='0.975')
        self.ButtonOF.on_clicked(self.callbackOF)

    def create_ShortFlowsButton(self):
        axSFButton = plt.axes([0.6, 0.1, 0.2, 0.05], axisbg=self.axcolor)
        self.ButtonSF = Button(axSFButton, '10x Shorter Flows', color=self.axcolor, hovercolor='0.975')
        self.ButtonSF.on_clicked(self.callbackSF)

    def callbackOF(self, event):
        self.flowScale = 1
        self.ButtonOF.color = 'green'
        self.ButtonSF.color = self.axcolor
        self.fig.canvas.draw()

    def callbackSF(self, event):
        self.flowScale = 0.1
        self.ButtonOF.color = self.axcolor
        self.ButtonSF.color = 'green'
        self.fig.canvas.draw()

    """
    Programs the FPGA with the bitstream in: 
    ../NetFPGA-SUME-live/projects/newFrugal_switch/bitfiles/newFrugal_v2_tuple_engines.bit
    """  
    def program_fpga(self):
        command = ['xmd', '-tcl', 'program_fpga.tcl']
        self.runCommand(command) 
    
    """
    Sets up the envoronment to run the MOONGEN application:
    removes hugtables
    binds the NIC interfaces to the DPDK drivers
    """
    def setup_PERC(self):
        command = ['rm', '-rf', '/mnt/huge/*']
        self.runCommand(command)
        command = ['rm', '-rf', '/dev/hugepages/*']
        self.runCommand(command)
        command = [self.setup_hugetables_script]
        self.runCommand(command)
        command = [self.insert_DPDK_module_script]
        self.runCommand(command)
        command = [self.NIC_driver_script, '--unbind'] + self.NIC_pcie_slots
        self.runCommand(command)
        command = [self.NIC_driver_script, '--bind', 'igb_uio'] + self.NIC_pcie_slots
        self.runCommand(command)

    """
    Runs the MoonGen PERC application with the desired flow scaling factor and reports the
    FCT results for small, meduim, and large flows
    """
    def run_PERC(self, flowScale):
        # run the perc script
        # command = ['python', self.perc_script_file, '--rtt', '0.01', '--interArrivalTime', '0.5', '--scaling', str(flowScale), '--percswitchDir', self.percswitchDir, '--moongenDir', self.moongenDir]
        home = "/home/sibanez"
        moonGen = "%s/build/MoonGen"%self.moongenDir
        mainFile = "%s/examples/perc-moongen-single/main1.lua"%self.moongenDir
        cdfFile = "%s/examples/perc-moongen-single/DCTCP_CDF"%self.moongenDir
        scaling = str(100)
        interArrivalTime = str(0.5)
        numFlows = str(120)
        fctFile = "%s/demo/%s"%(self.percswitchDir, self.results_file)
        
        command = ["sudo", "timeout", "60", moonGen, mainFile, 'single', cdfFile, scaling, interArrivalTime, numFlows]
        self.runCommand(command, working_directory=self.moongenDir)

        command = ['python', self.perc_script_file]
        self.runCommand(command, working_directory=self.moongenDir)

        new_fct_vals = self.read_results_file()
        return new_fct_vals

    def read_results_file(self): 
        # get the results from the results_file
        with open(self.results_file, 'rb') as f:
            reader = csv.reader(f)
            # set default values to be current values
            [fct_small, fct_medium, fct_large]  = self.perc_fct_current
            rowID = 0
            for row in reader:
                if len(row) == 6:
                    (current_rate, current_alg, fct_small, fct_medium, fct_large, updateID) = \
                    (int(row[0]), row[1], float(row[2]), float(row[3]), float(row[4]), int(row[5]))
                else:
                    print >> sys.stderr, "WARNING: unexpected results file format"
                if (rowID > 0):
                    print >> sys.stderr, "WARNING: more than one line in the results file"
                rowID += 1
        if (rowID == 0):
            print >> sys.stderr, "WARNING: no entries found in the fct_file.csv"
        return [fct_small, fct_medium, fct_large]
 
    """
    Takes in a list of strings specifying a command to run, runs the command and prints the output
    to std output
    """
    def runCommand(self, command, working_directory='.', shell=False):
        print '---------------------------------------'
        print "Running command: $ ", ' '.join(command)
        print '---------------------------------------'
        p = subprocess.Popen(' '.join(command), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=working_directory, shell=True)
        for line in iter(p.stdout.readline, ''):
            sys.stdout.write(line)
            sys.stdout.flush()
        p.wait() 


def main():
    fig, ax = plt.subplots() 
    gui = GUI(fig, ax)
    anim = animation.FuncAnimation(fig, gui.run, gui.data_gen, blit=False, init_func=gui.init_plot,
                                   repeat=False, interval=5000)
    plt.show()

if __name__ == "__main__":
    main()

"""
NOTES from Lavanya:
On the DCA server, you'll also have to remove any used huge pages from previous application and configure new huge pages the first time you run
rm -rf /mnt/huge/*
rm -rf /dev/hugepages/*
$MOONGEN_DIR/setup-hugetlbfs.sh
I think that server had 2MB pages, so you'd have to change "echo 4" and "hugepages-1048576kB" in the setup-hugetlbfs script to "echo 512" and "hugepages-2048kB"
"""


