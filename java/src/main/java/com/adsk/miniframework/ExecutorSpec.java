package com.adsk.miniframework;

import com.adsk.miniframework.webapp.Serializers.ExecutorSpecSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;


//
// - Wrapper for ExecutorInfo; mesos documentation requests for Executors to be wrapped
// and not extended
// - TODO use Custom Serializer (in webapp.Serializers); there's a bug in it that I haven't solved
//
//@JsonSerialize(using = ExecutorSpecSerializer.class)
public class ExecutorSpec
{
	public ExecutorInfo executor;
	
	//
	// - required resources per task
	// - plus # of instances
	//
	private double reqCpu;
	private double reqMem;
	private int instances;
	
	//
	// - Tasks launched by executor; decreased when the task is running
	// - Tasks running; increased when launch is complete, decreased when task is stopped
	//
	
	protected List<TaskID> tasksLaunched;
	protected List<TaskID> tasksRunning;
	
	//
	// - Verbatim json payload sent to each task
	//
	private JsonNode verbatim;

	//
	// - Constructor for Docker
	// - Also follows exactly the pre-baked Dockerfile. 
	//
	public ExecutorSpec(String executorName, String executorImage, String command, 
						boolean forcePull, double reqCpu, double reqMem, int instances, JsonNode verbatim) throws Exception
	{
		//
		// - Calls below constructor, then builds the executorinfo with the right containeriser
		//
		this(null, reqCpu, reqMem, instances, verbatim);
		
		//
		// - Build docker & container info
		//
		ContainerInfo.DockerInfo docker = ContainerInfo.DockerInfo.newBuilder()
										.setForcePullImage(forcePull)
										.setImage(executorImage)
										.setNetwork(ContainerInfo.DockerInfo.Network.HOST)
										.build();
		
		ContainerInfo container = ContainerInfo.newBuilder()
								.setType(ContainerInfo.Type.DOCKER)
								.setDocker(docker)
								.build();
	    
		//
		// - Set command to run
		//
	    CommandInfo cmdInfo = CommandInfo.newBuilder().setValue(command).build();
		
		//
		// - Stick this in the executor
		//
	    this.executor = ExecutorInfo.newBuilder()
	                            .setExecutorId(ExecutorID.newBuilder().setValue(executorName))
	                            .setCommand(cmdInfo)
	                            .addResources(Resource.newBuilder()
	                                      .setName("cpus")
	                                      .setType(Value.Type.SCALAR)
	                                      .setScalar(Value.Scalar.newBuilder().setValue(this.getRequiredCpu())))
		                        // Do the same for required mem
		                        .addResources(Resource.newBuilder()
		                                      .setName("mem")
		                                      .setType(Value.Type.SCALAR)
		                                      .setScalar(Value.Scalar.newBuilder().setValue(this.getRequiredMem())))
	                            .setName(executorName)
	                            .setSource("java")
	                            .setContainer(container)
	                            .build();
	}
	
	//
	// - Also accept pre-constructed executors
	// - IMPORTANT: note that taskImage is _not_ the executor image.
	//
	public ExecutorSpec(ExecutorInfo executorInfo, double reqCpu, double reqMem, int instances, JsonNode verbatim)
	{
		this.tasksLaunched = new ArrayList<TaskID>();
		this.tasksRunning = new ArrayList<TaskID>();
		
		this.reqCpu = reqCpu;
		this.reqMem = reqMem;
		this.instances = instances;
		this.verbatim = verbatim;
		
		this.executor = executorInfo;		
	}
	
	//
	// - Use this to kill all tasks gracefully with a payload
	// - Note: SchedulerDriver.killTask() is not reliable, so we have to spin our own
	// - solution for both graceful and reliable shutdown of tasks
	
	//
	// - This will make the actors cascade-suicide
	//	
//	for (String task : this.registered.get(appName).getRunningTasks())
//	{
//		JSONObject msg = new JSONObject();
//		msg.put("stop", "true");
//		msg.put("task", task);
//		byte[] bytes = new byte[0];
//		
//		try
//		{
//			bytes = msg.toString().getBytes("UTF-8");
//		}
//		catch(UnsupportedEncodingException e)
//		{
//			System.out.println("UnsupportedEncodingException in terminating application tasks.");
//			driver.abort();
//		}
//		
//		ExecutorID executor = this.tasks.get(task).getExecutor().getExecutorId();
//		SlaveID slave = this.tasks.get(task).getSlaveId();
//			    		
//		driver.sendFrameworkMessage(executor, slave, bytes);
//	}
	
	public void stopAllTasks(SchedulerDriver driver, byte[] payload)
	{
		for (TaskID task: this.tasksRunning)
		{
			//
			// - TODO will need to implement graceful shutdowns here, probably some sort of future
			// - sent to the imbedded miniakka might do. However receiving messages here won't be straightforward.
			//
			// driver.sendFrameworkMessage(this.executor.getExecutorId(), slave, payload);
			Status state = driver.killTask(task);
		}
	}
	
	public void putRunningTask(TaskID task)
	{
		this.tasksRunning.add(task);
	}
	
	public void putLaunchedTask(TaskID task)
	{
		this.tasksLaunched.add(task);
	}
	
	public void removeLaunchedTask(TaskID task)
	{
		this.tasksLaunched.remove(task);
	}
	
	public void removeRunningTask(TaskID task)
	{
		this.tasksRunning.remove(task);
	}
	
	//
	// - Various getters for info
	//
	public int getNumLaunched()
	{
		return this.tasksLaunched.size();
	}
	
	public int getNumRunning()
	{
		return this.tasksRunning.size();
	}
	
	public List<TaskID> getLaunchedTasks()
	{
		return this.tasksLaunched;
	}
	
	public List<TaskID> getRunningTasks()
	{
		return this.tasksRunning;
	}
	
	public double getRequiredCpu()
	{
		return this.reqCpu;
	}
		
	public double getRequiredMem()
	{
		return this.reqMem;
	}
	
	public int getRequiredInstances()
	{
		return this.instances;
	}
	
}
