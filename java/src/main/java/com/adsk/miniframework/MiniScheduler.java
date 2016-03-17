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
    // - Dict containing registered applications (name: appspec)
    // 
    private HashMap<String, AppSpec> registered;
    private double cpuLimit;
    private double memLimit;
    private int instanceLimit;
    
    //
    // - Dict from taskids to app names
    //
    private HashMap<String, String> tasks;
    
    // 
    // - book keeping constants used by e.g. framework
    // 
    private boolean implicitAcknowledgements;
   
    //
    // - a toy scheduler with some limits and a couple of specs
    //
    public MiniScheduler(boolean implicitAcknowledgements)
    {
    	this(implicitAcknowledgements, new HashMap<String, AppSpec>(), 5, 3, 256);
    }
    
    //
    // - External construction of apps
    //
    public MiniScheduler(boolean implicitAcknowledgements, HashMap<String, AppSpec> teams)
    {
    	//
    	// - Just use these limitations for now
    	//
        this(implicitAcknowledgements, teams, 5, 3, 256);
    }
    
    //
    // - Proper constructor; specify everything
    //
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
        this.tasks = new HashMap<String, String>();
        
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
    	// - Update specs for registered app
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
        // - Wish there were more documentation on the Offer class...
        // - TODO use allocator here
        //
        for (Offer offer : offers)
        {
        	MiniAllocator.naieveAllocate(driver, offer, this.registered, instanceLimit, cpuLimit, memLimit);
        }
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status)
    {

        // 
        // - Get the application name by splitting the task # off
        // 
        String[] splitted = status.getTaskId().getValue().split("-");
        String name = String.join("-", Arrays.copyOfRange(splitted, 0, splitted.length - 1));
        String executorName = status.getExecutorId().getValue();
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
        	this.registered.get(name).putRunningTask(executorName, status.getTaskId());
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
            this.registered.get(name).putStoppedTask(executorName, status.getTaskId());         
        }
        
        //
        // - TODO, don't abort the driver. Initiate task reconciliation here
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
        this.terminateApp(driver, name);
        this.terminateFramework(driver);
        
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
    // - TODO replace with termination function in appspec
    //
    public void terminateApp(SchedulerDriver driver, String appName)
    {
    	//
    	// - Only have to modify this boolean condition for whatever termination condition is needed
    	//
    	if (this.registered.get(appName).getNumRunning() >= 3)
		{   		
    		//
    		// = Update our application spec with termination flag
    		//
    		this.registered.get(appName).terminateApp(driver, new byte[0]);;
		}
    }
    
    //
    // - On task termination, see if that was the last task for the last app
    //
    public void terminateFramework(SchedulerDriver driver)
    {
        //
        // - Check if all the apps are finished (yes it's not the fastest check... but will do for now)
        //
        boolean allAppsTerminated = true;
        boolean allTasksStopped = true;
        
        for (AppSpec spec : this.registered.values())
        {
        	if(!spec.getAppTerminated())
    		{
        		allAppsTerminated = false;
    			break;
    		}
        	
        	if(spec.getNumRunning()!= 0)
        	{
        		allTasksStopped = false;
        		break;
        	}
        }
        
        //
        // - All done; we're finished
        // - Important: wait for all tasks to register as finished first.
        //
        if (allAppsTerminated && allTasksStopped)
        {
        	System.out.println("All tasks complete. Driver terminating.");
            driver.stop();
        }
    }
}