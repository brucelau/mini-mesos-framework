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
    private static TaskInfo buildTask(AppSpec aSpec, ExecutorSpec eSpec, Offer offer)
    {
    	
        // 
        // - Set task ID as name + number launched 
        // 
        TaskID taskID = TaskID.newBuilder().setValue(aSpec.name + "-" + UUID.randomUUID()).build();
        
		//
		// - Build docker & container info; there has to be a different dockerinfo for tasks
        // - because the executor will use a pre-baked image
        // - the task will use an extensible image
		//
		ContainerInfo.DockerInfo docker = ContainerInfo.DockerInfo.newBuilder()
										.setForcePullImage(true)
										.setImage(eSpec.getTaskImage())
										.setNetwork(ContainerInfo.DockerInfo.Network.HOST)
										.build();
		
		ContainerInfo container = ContainerInfo.newBuilder()
								.setType(ContainerInfo.Type.DOCKER)
								.setDocker(docker)
								.build();
		
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
                        .setContainer(container)
                        .setExecutor(ExecutorInfo.newBuilder(eSpec.executor))
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
	public static void naieveAllocate(SchedulerDriver driver, 
												Offer offer, 
												HashMap<String, AppSpec> apps, 
												int instanceLimit, 
												double cpuLimit, 
												double memLimit)
	{
		//
		// - Tasks that we will launch
		//
		List<TaskInfo> tasks = new ArrayList<TaskInfo>();
		
        // 
        // - Shuffle the list of specs, for fairness (will be greedy after shuffle)
        // 
        List<AppSpec> shuffled = new ArrayList<AppSpec>(apps.values());
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
        for (AppSpec aSpec : shuffled)
        {   
        	
        	//
        	// - if the application is terminated, or
        	// - if the team is already at quota,
        	// continue
        	//
        	if (aSpec.getAppTerminated() ||
        			aSpec.getCpuUsed() >= cpuLimit || 
            		aSpec.getMemUsed() >= memLimit)
        	{
        		continue;
        	}
        	
        	//
        	// - look through each executor wrapper
        	//
        	for (ExecutorSpec eSpec : aSpec.getExecutors().values())
        	{
	
	            // 
        		// if offer isn't enough for task, 
	            // - continue
	            // 
	            if (eSpec.getNumRunning() >= instanceLimit || 
	            		
	            		eSpec.getRequiredCpu() > remainingCpu || 
	            		eSpec.getRequiredMem() > remainingMem)
	            {
	                continue;
	            }
	            
	            //
	            // - Build the task in our naieve algorithm; pass the application, executor, offer along
	            //
	            TaskInfo task = buildTask(aSpec, eSpec, offer);
		    
	            //
	            // - Update list of tasks to be sent to driver
	            //
	            tasks.add(task);
	            
	            //
	            // - Set the task as being launched in records
	            //
	            aSpec.putLaunchedTask(eSpec.executor.getExecutorId().getValue(), task.getTaskId());
	            
	            System.out.println("Launching task " + task.getTaskId().getValue() + " using offer " + offer.getId().getValue());

	            remainingCpu -= eSpec.getRequiredCpu();
	            remainingMem -= eSpec.getRequiredMem();
        	}
        }
        
        //
        // - Accept the resource offer
        //
        acceptOffer(tasks, driver, offer);   
	}
}
