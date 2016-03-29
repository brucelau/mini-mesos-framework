package com.adsk.miniframework;

import com.adsk.miniframework.webapp.Serializers.*;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class MiniScheduler implements Scheduler
{
	//
	// - Json parser
	//
	private static final ObjectMapper mapper = makeMapper();
	
	private static ObjectMapper makeMapper()
	{
		ObjectMapper mapper = new ObjectMapper();
        
        //
        // - Just register the serializers here.
        //
    	SimpleModule module = new SimpleModule();
    	module.addSerializer(Application.class, new ApplicationSerializer());
    	module.addSerializer(ExecutorSpec.class, new ExecutorSpecSerializer());
    	
    	mapper.registerModule(module);
    	return mapper;
	}
	
    // 
	// - Members for applications
    // - Dict containing registeredApps applications (name: Application)
    // 
    private HashMap<String, Application> registeredApps;
    private double cpuLimit;
    private double memLimit;
    private int instanceLimit;
    
    //
    // - Dict from taskids to app names
    //
    private HashMap<String, String> tasksToApps;
    
    //
    // - See mesos documentation on this one
    //
    private boolean implicitAcknowledgements;
   
    //
    // - a toy scheduler with some limits and a toy ubuntu task for the executor to run
    //
    public MiniScheduler(boolean implicitAcknowledgements)
    {
    	this(implicitAcknowledgements, new HashMap<String, Application>(), 5, 3, 256);
    	try
    	{
	    	Application app1 = new Application("app1");
	    	ExecutorSpec exe1 = new ExecutorSpec("exec1",
	    										"lmok/mini-executor",
	    										"/opt/docker_executor",
	    										false,
	    										1.0,
	    										32.0,
	    										1,
	    										mapper.createObjectNode());
	    	app1.putExecutorSpec(exe1);
	    	this.registeredApps.put(app1.name, app1);
	    	
    	}
    	catch (Exception e)
    	{
    		System.out.println("Could not create toy executor");
    	}
    }
    
    //
    // - External construction of app
    //
    public MiniScheduler(boolean implicitAcknowledgements, HashMap<String, Application> teams)
    {
    	//
    	// - Just use these limitations for now
    	//
        this(implicitAcknowledgements, teams, 5, 3, 256);
    }
    
    //
    // - Proper constructor; specify everything
    //
    public MiniScheduler(boolean implicitAcknowledgements, HashMap<String, Application> teams, int instanceLimit, double cpuLimit, double memLimit)
    {
        
        this.implicitAcknowledgements = implicitAcknowledgements;
        
        // 
        // - Resource limits per registeredApps app (may be one task)
        // 
        this.instanceLimit = instanceLimit;
        this.cpuLimit = cpuLimit;
        this.memLimit = memLimit;
        this.registeredApps = new HashMap<String, Application>(teams);
        this.tasksToApps = new HashMap<String, String>();
    }
    
    public Application getSpecs(String name)
    {
    	//
    	// - Gets specs for registeredApps app
    	//
        return registeredApps.get(name);
    }

    public void putSpecs(String name, Application spec)
    {
    	//
    	// - Update specs for registeredApps app
    	//
        registeredApps.put(name, spec);
    }
    
    @Override
    public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo)
    {
        System.out.println("registered framework: " + frameworkId.getValue());
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo){
        System.out.println("driver reregistered");
    }

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
        // - Use allocation scheme here. The allocator has to update the # of launched tasks.
        //
        for (Offer offer : offers)
        {
        	MiniAllocator.naieveAllocate(driver, offer, this.registeredApps, this.tasksToApps, instanceLimit, cpuLimit, memLimit);
        }
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status)
    {
    	
        // 
        // - Get the application name using the reverse search dict
        // 
    	String appName = this.tasksToApps.get(status.getTaskId().getValue());
        String executorName = status.getExecutorId().getValue();
        
        //
        // - Get the message from the status update
        //
        try
        {
			JsonNode msgJson = DockerExecutor.getObjectMapper().readValue(status.getMessage(), JsonNode.class);
        }
        catch (Exception e)
        {
        	// Don't bother handling it right now
        }
        
        if (status.getState() == TaskState.TASK_RUNNING)
        {
        	this.registeredApps.get(appName).putRunningTask(executorName, status.getTaskId());
        }
        // 
        // - Finished task; retrieve the name and update the app registration
        // 
        else if (status.getState() == TaskState.TASK_FINISHED)
        {
        	
            //
        	// - Tell the spec that a task has finished
            // - If the task is the app's final task (find it in the Json message),
            // - update the app to be terminated. See Application.taskStopped().
            //
            this.registeredApps.get(appName).putStoppedTask(executorName, status.getTaskId());         
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
            // - otherwise will have to update registeredApps specs
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
        this.terminateApp(driver, appName);
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
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) 
    {
    	System.out.println("offer rescinded");
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) 
    {
    	System.out.println("framework message: " + new String(data, StandardCharsets.UTF_8));
    }

    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) 
    {
    	System.out.println("--> Slave " + slaveId.getValue() + " lost");
    }

    @Override
    public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, int status) 
    {
    	System.out.println("--> Executor " + executorId.getValue() + " lost with status " + status);
    }

    public void error(SchedulerDriver driver, String message)
    {
        System.out.println("Error: " + message);
    }
    
    //
    // - On status update, this checks if the task's application
    // - is entirely finished. Our e.g. condition is to stop any application with 2 running tasks.
    // - TODO replace with termination function in Application
    //
    public void terminateApp(SchedulerDriver driver, String appName)
    {
    	//
    	// - Only have to modify this boolean condition for whatever termination condition is needed
    	//
    	if (this.registeredApps.get(appName).getNumRunning() >= 3)
		{   		
    		//
    		// = Update our application spec with termination flag
    		//
    		System.out.println("Sending kill to " + appName);
    		this.registeredApps.get(appName).terminateApp(driver, new byte[0]);;
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
        
        for (Application spec : this.registeredApps.values())
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
    
    //
    // - Methods for the REST api
    //
    public List<String> getRegisteredAppNames()
    {
    	List<String> names = new ArrayList<String>();
    	names.addAll(this.registeredApps.keySet());
    	return names;
    }
    
    //
    // Returns jsonnode of an app, rendered by our custom mapper
    //
    public JsonNode getRegisteredApp(String appName)
    {
    	return mapper.valueToTree(this.registeredApps.get(appName));
    }
    
    
    public void registerApp(Application app)
    {
    	this.registeredApps.put(app.name, app);
    	//return this.registeredApps.containsKey(app.name);
    }
    
    //
    // - Get our global jackson object mapper
    //
    public static ObjectMapper getObjectMapper()
    {
    	return mapper;
    }
}