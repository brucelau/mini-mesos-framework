#
# - Generic apache license goes here
#

#
# - LM: Based off of the example python framework with mesos, and
# - the one at http://jamesporter.me/2014/11/15/hello-mesos.html
# - I may have stolen some code from ochopod
#

import sys
import threading
import time
import pykka
import json

from pykka.exceptions import ActorDeadError
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

PREFIX = "Executor ->"

def _kill(actor_ref):

    #
    # - From Olivier's code -- kill actor without callbacks
    #
    try:
        if not actor_ref:
            return False

        actor_ref.tell({'command': 'pykka_stop'})
        return True

    except ActorDeadError:
        pass

    return False

def _update(driver, task, state, data):
    
    #
    # - Update the scheduler with status
    #
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = state
    update.data = json.dumps(data)
    driver.sendStatusUpdate(update)

class Sleepy(pykka.ThreadingActor):

    def __init__(self, driver, task):

        super(Sleepy, self).__init__()
        self.driver = driver
        self.task = task
        self.prefix = 'Task %s %s' % (self.task.task_id.value, PREFIX)
        self.result = {}

    def on_start(self):

        print '%s Started actor %s' % (self.prefix, self.actor_urn)

        #
        # - Let scheduler know we've started
        #
        _update(self.driver, self.task, mesos_pb2.TASK_RUNNING, {'actor_id': self.actor_urn, 'task': 'started'})

        #
        # - Queue dummy on-start message
        #
        msg = self.actor_ref.tell({'idle': '%s' % self.task.task_id.value})        

    def on_receive(self, msg):

        #
        # - Leave actor idle
        #
        if 'idle' in msg:

            pass

        #
        # - this is for communication
        #
        elif 'poke' in msg:
            
            print 'Actor %s received message: %s.' % (self.actor_urn, json.dumps(msg))            

            #
            # - Queue the job, just for this example
            #
            self.actor_ref.tell({'run': ''})

            return {'poke': 'poke back from task %s' % self.task.task_id.value}

        #
        # - this is for running the actual job
        #
        elif 'run' in msg:

            #
            # - the job's to sleep, of course
            #
            time.sleep(2)
            self.result = {'darth': 'vader'}

            #
            # - done, queue stop message
            #
            self.stop()

        #
        # - Force kill the thing; use for task reconciliation
        #
        elif 'kill' in msg:

            _kill(self.actor_ref)

    def on_stop(self):

        print "%s Actor %s stopped successfully." % (self.prefix, self.actor_urn)

        #
        # - Let scheduler know we're done
        #
        _update(self.driver, self.task, mesos_pb2.TASK_FINISHED, {'actor_id': self.actor_urn, 'task': 'completed', 'output': self.result})

    def on_failure(self, exception_type, exception_value, traceback):

        #
        # - Diagnosis done for us by pykka
        #
        error = '%s %s %s' % (exception_type, exception_value, traceback)
        print "%s Actor %s stopped with failure %s." % (self.prefix, self.actor_urn, error)

        #
        # - Let scheduler know we're screwed
        #
        _update(self.driver, self.task, mesos_pb2.TASK_FAILED, {'actor_id': self.actor_urn, 'task': 'error', 'error': error})


class MiniExecutor(mesos.interface.Executor):

    def __init__(self):
        
        super(MiniExecutor, self).__init__()
        self.sleepies = {}

    def launchTask(self, driver, task):
        
        #
        # - Use pykka actors to run tasks
        #
        print "Task %s %s Running." % (task.task_id.value, PREFIX)

        try:

            sleepy = Sleepy.start(driver, task)
            self.sleepies[sleepy.actor_urn] = sleepy

        except Exception as e:

            #
            # - Diagnosis; find the line
            #
            error = "%s on line %s" % (e, sys.exc_info()[-1].tb_lineno)
            print 'Task %s %s: %s' % (task.task_id.value, PREFIX, error)
            driver.sendFrameworkMessage(json.dumps({'actor_id': None, 'error': '*** Failed to start actor ***: %s' % error}))
            raise e

    def frameworkMessage(self, driver, msg):
        
        #
        # - Let the scheduler poke a sleepy actor with a payload
        #
        reply = {}

        if not 'actor_id' in msg:

            reply['error'] = 'missing actor_id key'

        else:

            try:

                #
                # - Find the right actor
                #
                msg = json.loads(msg)
                reply = self.sleepies[msg['actor_id']].ask(msg, timeout=10)

            except ActorDeadError:

                print 'Task %s %s Actor %s is dead' % (task.task_id.value, PREFIX, msg['actor_id'])
                reply = {'actor_id': self.actor_urn, 'error': 'actor dead'}

        driver.sendFrameworkMessage(json.dumps(reply))

if __name__ == "__main__":

    print "Starting mini executor"
    driver = mesos.native.MesosExecutorDriver(MiniExecutor())    
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
