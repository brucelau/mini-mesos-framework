package com.adsk.miniframework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class MiniAllocator
{
	
	//
	// Builds a taskinfo with info provided
	//
    private static TaskInfo buildTask(Application app, ExecutorSpec eSpec, Offer offer)
    {
    	
        // 
        // - Set task ID as name + number launched 
        // 
        TaskID taskID = TaskID.newBuilder().setValue(app.name + "-" + UUID.randomUUID()).build();

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
                                      .setScalar(Value.Scalar.newBuilder().setValue(eSpec.getRequiredCpu())))
                        // Do the same for required mem
                        .addResources(Resource.newBuilder()
                                      .setName("mem")
                                      .setType(Value.Type.SCALAR)
                                      .setScalar(Value.Scalar.newBuilder().setValue(eSpec.getRequiredMem())))
                        //.setContainer(container)
                        //.setCommand(CommandInfo.newBuilder().setValue("echo").build())
                        .setExecutor(eSpec.executor)
                        .build();
        return task;
    }
    
    //
    // - Takes a list of taskinfo and sends it to the executor
    //
	private static void acceptOffer(List<TaskInfo> tasks, SchedulerDriver driver, Offer offer)
	{
        Offer.Operation.Launch.Builder launcher = Offer.Operation.Launch.newBuilder();

        for (TaskInfo task : tasks)
        {
        	launcher.addTaskInfos(TaskInfo.newBuilder(task));
        }
        // 
        // - This janky bit of code is from the example framework;
        // - will apparently deprecate SchedulerDriver.launchTasks()
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
	
    //
    // - Naieve allocation scheme.
    // - Want to return a list of taskinfo 
	//
	public static List<TaskInfo> naieveAllocate(SchedulerDriver driver, Offer offer, HashMap<String, Application> apps, HashMap<String, String> tasksToApps, 
										int instanceLimit, double cpuLimit, double memLimit)
	{
		//
		// - Tasks that we will launch
		//
		List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		
        // 
        // - Shuffle the list of specs, for fairness (will be greedy after shuffle)
        // 
        List<Application> shuffled = new ArrayList<Application>(apps.values());
        Collections.shuffle(shuffled);
		// 
        // - Build an offer launcher; functions just as queue of tasks
        // 
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
        for (Application app : shuffled)
        {   
        	
        	//
        	// - if the application is terminated, or
        	// - if the team is already at quota,
        	// continue
        	//
        	if (app.getAppTerminated() || app.getCpuUsed() >= cpuLimit || app.getMemUsed() >= memLimit)
        	{
        		continue;
        	}
        	
        	//
        	// - look through each executor wrapper
        	//
        	for (ExecutorSpec eSpec : app.getExecutors().values())
        	{
	
	            // 
        		// if offer isn't enough for task, 
        		// or if there are enough launched tasks queued
	            // - continue
	            // 
	            if (eSpec.getNumRunning() >= instanceLimit || eSpec.getRequiredCpu() > remainingCpu || eSpec.getRequiredMem() > remainingMem
	            		|| eSpec.getNumLaunched() >= eSpec.getRequiredInstances())
	            {
	                continue;
	            }
	            
	            //
	            // - Build the task in our naieve algorithm; pass the application, executor, offer along
	            //
	            TaskInfo task = buildTask(app, eSpec, offer);
		    
	            //
	            // - Update list of tasks to be sent to driver
	            //
	            tasks.add(task);
	            
	            //
	            // - Set the task as being launched for the app
	            //
	            app.putLaunchedTask(eSpec.executor.getExecutorId().getValue(), task.getTaskId());
	            
	            //
	            // - Update the hashmap for reverse search from task to app
	            //
	            tasksToApps.put(task.getTaskId().getValue(), app.name);
	            
	            System.out.println("Launching task " + task.getTaskId().getValue() + " using offer " + offer.getId().getValue());

	            remainingCpu -= eSpec.getRequiredCpu();
	            remainingMem -= eSpec.getRequiredMem();
        	}
        }
        
        //
        // - Accept the resource offer
        //
        acceptOffer(tasks, driver, offer);   
        
        return tasks;
	}

	//
	// - DRF Allocation
	//
	public static void drfAllocate(SchedulerDriver driver, Offer offer, HashMap<String, Application> apps, HashMap<String, String> tasksToApps, 
			int instanceLimit, double cpuLimit, double memLimit)
	{
		
	}
}
