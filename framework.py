#
# - Generic apache license goes here
#

#
# - LM: Based off of the example python framework with mesos, and
# - the one at http://jamesporter.me/2014/11/15/hello-mesos.html
#

import os
import sys
import time
import json

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

TOTAL_TASKS = 5

TASK_CPUS = 1
TASK_MEM = 64

PREFIX = "Scheduler ->"

class MiniScheduler(mesos.interface.Scheduler):

    def __init__(self, executor, implicit=False):

        self.implicit = implicit
        self.executor = executor
        self.task_data = {}
        self.tasks_launched = 0
        self.tasks_finished = 0
        self.tasks_failed = 0
        self.summary = {}

    def registered(self, driver, frameworkId, masterInfo):

        print "%s Registered with ID %s" % (PREFIX, frameworkId.value)

    def resourceOffers(self, driver, offers):
        
        #
        # - Take all offers; resource allocation algorithm from example python framework
        #
        for offer in offers:

            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            print "%s Received offer %s with cpus: %s and mem: %s" \
                  % (PREFIX, offer.id.value, offerCpus, offerMem)

            remainingCpus = offerCpus
            remainingMem = offerMem

            #
            # - This just creates tasks to the limit...
            #
            while self.tasks_launched < TOTAL_TASKS and remainingCpus >= TASK_CPUS and remainingMem >= TASK_MEM:

                tid = self.tasks_launched
                self.tasks_launched += 1

                print "%s Launching task %d using offer %s" % (PREFIX, tid, offer.id.value)
                
                #
                # - Create a new task
                #
                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid
                task.executor.MergeFrom(self.executor)

                #
                # - Update available resources
                #
                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = TASK_CPUS

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = TASK_MEM

                remainingCpus -= TASK_CPUS
                remainingMem -= TASK_MEM

                #
                # - Queue the task
                #
                tasks.append(task)
                self.task_data[task.task_id.value] = (
                    offer.slave_id, task.executor.executor_id)

            #
            # - Run whatever's in our queue
            #
            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):

        print "%s Task %s is in state %s" % (PREFIX, update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        #
        # - Check update from actor
        #
        slave_id, executor_id = self.task_data[update.task_id.value]

        data = json.loads(update.data) if update.data is not '' else {}

        #
        # - Executor initialised with an actor
        #
        if update.state == mesos_pb2.TASK_RUNNING:

            #
            # - Check the sleepy is running
            #
            if not 'actor_id' in data:
            
                print "%s Actor did not initialise properly (received %s)" % (PREFIX, update.data)
                sys.exit(1)

            print "%s Task %s Actor started: %s" % (PREFIX, update.task_id.value, update.data)

            #
            # - Poke the sleepy;
            #
            msg = {'poke': '', 'actor_id': data['actor_id']}
            msg = json.dumps(msg)
            driver.sendFrameworkMessage(executor_id, slave_id, msg)
        
        #
        # - Executor thinks that it's done
        #
        elif update.state == mesos_pb2.TASK_FINISHED:

            self.tasks_finished += 1
            
            #
            # - Want to process job end here
            #            
            print "%s Task %s Completion: %s" % (PREFIX, update.task_id.value, update.data)
            self.summary[update.task_id.value] = data

            #
            # - All done
            #
            if self.tasks_finished == TOTAL_TASKS:
                
                print "%s All tasks done. Shutting down..." % PREFIX
                driver.stop()

        #
        # - Ran into a bunch of problems, abort the driver entirely
        #
        elif update.state in (mesos_pb2.TASK_LOST, mesos_pb2.TASK_KILLED, mesos_pb2.TASK_FAILED):

            print "%s Aborting because task %s: %s with message '%s'" \
                % (PREFIX, update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
            
            self.tasks_failed += 1

            driver.abort()

        #
        # - For explicit/implicit acknowledgements; TODO task reconciliation
        #
        if not self.implicit:

            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):

        print "%s Received message: %s" % (PREFIX, message)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.command.value = os.path.abspath("./executor")
    executor.name = "Executor"
    executor.source = "lmok_python"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Framework"
    framework.checkpoint = True

    #
    # - Implicit acknowledgements
    #
    implicit = False if not os.getenv("IMPLICIT") else bool(os.getenv("IMPLICIT"))

    #
    # - Get credentials
    #
    credential = mesos_pb2.Credential()

    #
    # - If specified by env
    #
    if os.getenv("PRINCIPAL"):

        credential.principal = os.getenv("PRINCIPAL")
        credential.secret = os.getenv("SECRET")
        framework.principal = os.getenv("PRINCIPAL")

    #
    # - Try to get it from a cred file (yaml for now, not json)
    #
    else:

        if not os.getenv("CREDENTIAL"):
            
            print "No commandline credentials or credential file provided."
            sys.exit(1)

        try:

            cred_path = os.path.join(os.getcwd(), os.getenv("CREDENTIAL"))

            with open(cred_path) as cred_file:

                principal, secret = cred_file.readline()
                credential.principal = principal
                framework.principal = principal
                credential.secret = secret

        except Exception as e:

            print e
            sys.exit(1)

    #
    # - Create the Scheduler driver (communicates between framework + mesos)
    #
    driver = mesos.native.MesosSchedulerDriver(
        MiniScheduler(executor, implicit),
        framework,
        sys.argv[1],
        implicit,
        credential)

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop();

    sys.exit(status)
