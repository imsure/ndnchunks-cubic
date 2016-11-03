#!/usr/bin/python

from ndn.experiments.experiment import Experiment
import time

class NDNChunkExperiment(Experiment):
    def __init__(self, args):
        Experiment.__init__(self, args)

    def setup(self):
        for host in self.net.hosts:
            # Set strategy
            host.nfd.setStrategy("/ndn/edu", self.strategy)

            if host.name.startswith('producer'):
                host.cmd('ndnputchunks -s 1024 ndn:/ndn/edu/%s/spmcat < /vagrant/spmcat > producer_log 2>&1 &' % host.name)

        # Wait for convergence time period
        print "Waiting " + str(self.convergenceTime) + " seconds for convergence..."
        time.sleep(self.convergenceTime)
        print "...done"

        # To check whether all the nodes of NLSR have converged
        didNlsrConverge = True

        # Checking for convergence
        for host in self.net.hosts:
            statusRouter = host.cmd("nfd-status -b | grep /ndn/edu/%C1.Router/cs/")
            statusPrefix = host.cmd("nfd-status -b | grep /ndn/edu/")
            didNodeConverge = True
            for node in self.nodes.split(","):
                    if ("/ndn/edu/%C1.Router/cs/" + node) not in statusRouter:
                        didNodeConverge = False
                        didNlsrConverge = False
                    if str(host) != node and ("/ndn/edu/" + node) not in statusPrefix:
                        didNodeConverge = False
                        didNlsrConverge = False

            host.cmd("echo " + str(didNodeConverge) + " > convergence-result &")

        if didNlsrConverge:
            print("NLSR has successfully converged.")
        else:
            print("NLSR has not converged. Exiting...")
            for host in self.net.hosts:
                host.nfd.stop()
            sys.exit(1)


    def run(self):
        for host in self.net.hosts:
            if host.name.startswith('consumer'):
                host.cmd("ndncatchunks -v -t cubic --cubic-debug-cwnd /tmp/%s/cwnd.txt --cubic-debug-rtt /tmp/%s/rtt.txt -d iterative ndn:/ndn/edu/producer%s/spmcat 2> consumer_log 1> spmcat &" % (host.name, host.name, host.name[-1]))

Experiment.register("ndnchunk-cubic", NDNChunkExperiment)
