package com.adsk.miniframework;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

public class MiniScheduler implements Scheduler
{
	//
	// - Json parser
	//
	private JSONParser parser;
	
    // 
	// - Members for applications
    // - Dict containing registered applications
    // 
    private HashMap<String, AppSpec> registered;
    private double cpuLimit;
    private double memLimit;
    private int instanceLimit;
    
    //
    // - Dict containing launched tasks
    //
    private HashMap<String, TaskInfo> tasks;
    
    // 
    // - book keeping constants used by e.g. framework
    // 
    private final boolean implicitAcknowledgements;
   
    public MiniScheduler(boolean implicitAcknowledgements, HashMap<String, AppSpec> teams)
    {
    	//
    	// - Just use these limitations for now
    	//
        this(implicitAcknowledgements, teams, 5, 3, 256);
    }

    public MiniScheduler(boolean implicitAcknowledgements, HashMap<String, AppSpec> teams, int instanceLimit, double cpuLimit, double memLimit)
    {
        
        this.implicitAcknowledgements = implicitAcknowledgements;
        this.parser = new JSONParser();
        
        // 
        // - Resource limits per registered app (may be one task)
        // 
        this.instanceLimit = instanceLimit;
        this.cpuLimit = cpuLimit;
        this.memLimit = memLimit;
        this.registered = new HashMap<String, AppSpec>(teams);
        this.tasks = new HashMap<String, TaskInfo>();
    }

    public AppSpec getSpecs(String name)
    {
    	//
    	// - Gets specs for registered app
    	//
        return registered.get(name);
    }

    public void putSpecs(String name, AppSpec spec)
    {
    	//
    	// - Update specs for registered app (deprecated?)
    	//
        registered.put(name, spec);
    }

    @Override
    public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo)
    {
        System.out.println("Registered! ID = " + frameworkId.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo){}

    @Override
    public void disconnected(SchedulerDriver driver) 
    {
        System.out.println("Driver disconnection.");
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Offer> offers)
    {

        // 
        // - Shuffle the list of specs, for fairness (will be greedy after shuffle)
        // 
        List<AppSpec> shuffled = new ArrayList<AppSpec>(this.registered.values());
        Collections.shuffle(shuffled);

        //
        // - Same offer iteration as python example
        // - Wish there were more documentation on the Offer class...
        //
        for (Offer offer : offers)
        {
            // 
            // - Build an offer launcher; functions just as queue of tasks
            // 
            Offer.Operation.Launch.Builder launcher = Offer.Operation.Launch.newBuilder();
            double offerCpu = 0;
            double offerMem = 0;
            
            //
            // - Resources for a particular offer; count resources in offer
            // - would want to count the instances (though one offer is from one slave)
            // 
            for (Resource resource : offer.getResourcesList())
            {
                if (resource.getName().equals("cpus"))
                {
                    offerCpu += resource.getScalar().getValue();
                }
                else if (resource.getName().equals("mem"))
                {
                    offerMem += resource.getScalar().getValue();
                }
            }

            System.out.println("Received offer " + offer.getId().getValue() + " with cpus: " + offerCpu + " and mem: " + offerMem);

            double remainingCpu = offerCpu;
            double remainingMem = offerMem;

            // 
            // - Main loop queueing tasks
            // 
            for (AppSpec spec : shuffled)
            {   

                // 
                // - if team is over quota, 
                // - or offer isn't enough for task, 
            	// - or the application's finished:
                // - continue
                // 
                if (spec.jsonGetInt("allocated_instances") >= this.instanceLimit || 
                		spec.jsonGetDouble("allocated_cpus") >= this.cpuLimit || 
                		spec.jsonGetDouble("allocated_mem") >= this.memLimit ||
                		spec.jsonGetDouble("required_cpus") > remainingCpu || 
                		spec.jsonGetDouble("required_mem") > remainingMem ||
                		spec.jsonGetBoolean("app_terminated"))
                {
                    continue;
                }

                // 
                // - Set task ID as name + number launched 
                // 
                TaskID taskID = TaskID.newBuilder().setValue(spec.name + "-" + spec.jsonGetString("launched_tasks")).build();

                System.out.println("Launching task " + taskID.getValue() + " using offer " + offer.getId().getValue());
                               
                // 
                // - Queue the task onto the launcher
                // 
                TaskInfo task = TaskInfo.newBuilder()
                                .setName("task " + taskID.getValue())
                                .setTaskId(taskID)
                                .setSlaveId(offer.getSlaveId())
                                // Get the required cpus off of the specs
                                .addResources(Resource.newBuilder()
                                              .setName("cpus")
                                              .setType(Value.Type.SCALAR)
                                              .setScalar(Value.Scalar.newBuilder().setValue(spec.jsonGetDouble("required_cpus"))))
                                // Do the same for required mem
                                .addResources(Resource.newBuilder()
                                              .setName("mem")
                                              .setType(Value.Type.SCALAR)
                                              .setScalar(Value.Scalar.newBuilder().setValue(spec.jsonGetDouble("required_mem"))))
                                .setExecutor(ExecutorInfo.newBuilder(spec.executor))
                                .build();

                launcher.addTaskInfos(TaskInfo.newBuilder(task));
                
                //
                // - Update list of tasks launched
                //
                this.tasks.put(task.getTaskId().getValue(), task);

                // 
                // - Increment the runs on the spec
                // 
                this.registered.get(spec.name).taskLaunched(taskID.getValue());

                remainingCpu -= spec.jsonGetDouble("required_cpus");
                remainingMem -= spec.jsonGetDouble("required_mem");
            }

            // 
            // - This janky bit of code is just from the example framework;
            // - will apparently be deprecated
            // 
            List<OfferID> offerIds = new ArrayList<OfferID>();
            offerIds.add(offer.getId());

            List<Offer.Operation> operations = new ArrayList<Offer.Operation>();

            Offer.Operation operation = Offer.Operation.newBuilder()
                                        .setType(Offer.Operation.Type.LAUNCH)
                                        .setLaunch(launcher)
                                        .build();

            operations.add(operation);

            Filters filters = Filters.newBuilder().setRefuseSeconds(1).build();

            // 
            // - Accept the offer with our queued task
            // 
            driver.acceptOffers(offerIds, operations, filters);
        }
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status)
    {

        // 
        // - Get the application name by splitting the task # off
        // 
        String taskID = status.getTaskId().getValue();
        String[] splitted = taskID.split("-");
        String name = String.join("-", Arrays.copyOfRange(splitted, 0, splitted.length - 1));
        
        //
        // - Get the message from the status update
        //
        JSONObject msgJson = new JSONObject();
        try
        {
        	msgJson = (JSONObject) this.parser.parse(status.getMessage());
        }
        catch (ParseException e)
        {
        	// Don't bother handling it right now
        }
        
        if (status.getState() == TaskState.TASK_RUNNING)
        {
        	this.registered.get(name).tasksRunning(taskID);
        }
        // 
        // - Finished task; retrieve the name and update the app registration
        // 
        else if (status.getState() == TaskState.TASK_FINISHED)
        {
        	
            //
        	// - Tell the spec that a task has finished
            // - If the task is the app's final task (find it in the Json message),
            // - update the app to be terminated. See AppSpec.taskStopped().
            //
            this.registered.get(name).taskStopped(taskID);         
        }
        
        //
        // - Todo, don't abort the driver. Kill the task + app and continue with the rest.
        //
        else if (status.getState() == TaskState.TASK_LOST || status.getState() == TaskState.TASK_KILLED || status.getState() == TaskState.TASK_FAILED)
        {
            System.err.println("Aborting because task " + status.getTaskId().getValue() +
                               " is in unexpected state " +  status.getState().getValueDescriptor().getName() +
                               " with reason '" + status.getReason().getValueDescriptor().getName() + "'" +
                               " from source '" + status.getSource().getValueDescriptor().getName() + "'" +
                               " with message '" + status.getMessage() + "'");

            // 
            // - aborting driver shuts everything down;
            // - otherwise will have to update registered specs
            // 
            driver.abort();
        }
        
        // 
        // - Standard status update 
        // 
    	System.out.println("Status update: task " + status.getTaskId().getValue() +
                       " in state " + status.getState().getValueDescriptor().getName() +
                       " with message " + status.getMessage());

        //
        // - See if we should kill all tasks in the app
        // - then see if we have killed all tasks in all apps
        //
        this.appTerminate(driver, name);
        this.frameworkTerminate(driver);
        
        if (!implicitAcknowledgements)
        {
            driver.acknowledgeStatusUpdate(status);
        }
    }
    
    //
    // - We're not using these for now...
    //
    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {}

    @Override
    public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {}

    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {}

    @Override
    public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, int status) {}

    public void error(SchedulerDriver driver, String message)
    {
        System.out.println("Error: " + message);
    }
    
    //
    // - On status update, this checks if the task's application
    // - is entirely finished. Our e.g. condition is to stop any application with 2 running tasks.
    //
    public void appTerminate(SchedulerDriver driver, String appName)
    {
    	//
    	// - Only have to modify this boolean condition for whatever termination condition is needed
    	//
    	if (this.registered.get(appName).jsonGetInt("running_tasks") >= 3)
		{
    		//
    		// - This well cascade the actors to suicide
    		//
    		for (String task : this.registered.get(appName).getTasksRunning())
    		{
	    		JSONObject msg = new JSONObject();
	    		msg.put("stop", "true");
	    		msg.put("task", task);
	    		byte[] bytes = new byte[0];
	    		
	    		try
	    		{
	    			bytes = msg.toString().getBytes("UTF-8");
	    		}
	    		catch(UnsupportedEncodingException e)
	    		{
	    			System.out.println("UnsupportedEncodingException in terminating application tasks.");
	    			driver.abort();
	    		}
	    		
	    		ExecutorID executor = this.tasks.get(task).getExecutor().getExecutorId();
	    		SlaveID slave = this.tasks.get(task).getSlaveId();
	    		
//	    		System.out.println("Stopping  task: " + task + " executor: " + executor + " on slave: " + slave);
	    		
	    		driver.sendFrameworkMessage(executor, slave, bytes);
    		}
    		
    		//
    		// = Update our application spec with termination flag
    		//
    		this.registered.get(appName).appTerminated();
		}
    }
    
    //
    // - On task termination, see if that was the last task for the last app
    //
    public void frameworkTerminate(SchedulerDriver driver)
    {
        //
        // - Check if all the apps are finished (yes it's not the fastest check... but will do for now)
        //
        boolean allTerminated = true;
        boolean noTasksLeft = true;
        
        for (AppSpec spec : this.registered.values())
        {
        	if(!spec.jsonGetBoolean("app_terminated"))
    		{
    			allTerminated = false;
    			break;
    		}
        	
        	if(spec.jsonGetInt("running_tasks") != 0)
        	{
        		noTasksLeft = false;
        		break;
        	}
        }
        
        //
        // - All done; we're finished
        // - Important: wait for all tasks to register as finished first.
        //
        if (allTerminated && noTasksLeft)
        {
        	System.out.println("All tasks complete. Driver terminating.");
            driver.stop();
        }
    }
}