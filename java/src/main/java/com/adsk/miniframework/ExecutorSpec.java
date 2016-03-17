package com.adsk.miniframework;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import org.apache.mesos.v1.Protos.ContainerInfo;
import org.json.simple.JSONObject;

//
// - Wrapper for ExecutorInfo
//
public class ExecutorSpec
{
    //
    // - A tiny message for graceful termination... at some point
    //
    public static class Terminate{}
    
	public ExecutorInfo executor;
	
	//
	// - required resources per task
	// - plus # of instances
	//
	private double reqCpu;
	private double reqMem;
	private int instances;
	
	//
	// - Tasks launched by this executor
	//
	private List<TaskID> tasksLaunched;
	private List<TaskID> tasksRunning;
	
	//
	// - Verbatim json payload sent to each task
	//
	private JSONObject verbatim;
	
	//
	// - Docker image
	//
	private final String taskImage;

	//
	// - May not have a json payload
	//
//	public ExecutorSpec(String name, String command, String path, String image, double reqCpu, double reqMem, int instances) throws Exception
//	{
//		this(name, command, path, image, reqCpu, reqMem, instances, new JSONObject());
//	}
//	
	//
	// - Constructor looks for the command to build an ExecutorInfo
	//
//	public ExecutorSpec(String executorName, String command, String path, String image, double reqCpu, double reqMem, int instances, JSONObject verbatim) throws Exception
//	{
//		this(null, reqCpu, reqMem, instances, verbatim);
//		
//	    CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).build();
//	    CommandInfo cmdInfo = CommandInfo.newBuilder().setValue(command).addUris(uri).build();
//	    
//	    this.executor = ExecutorInfo.newBuilder()
//	                            .setExecutorId(ExecutorID.newBuilder().setValue(executorName))
//	                            .setCommand(cmdInfo)
//	                            .setName(executorName)
//	                            .setSource("java")
//	                            .build();
//	}
	
	//
	// - Constructor for Docker -- Somewhat stolen from @podgorj
	// - Also follows exactly the pre-baked Dockerfile. 
	//
	public ExecutorSpec(String executorName, String executorImage, String taskImage, boolean forcePull, double reqCpu, double reqMem, int instances, JSONObject verbatim) throws Exception
	{
		//
		// - Calls below constructor, then builds the executorinfo with the right containeriser
		//
		this(null, taskImage, reqCpu, reqMem, instances, verbatim);
		
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
	    CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(new File("/opt/").getCanonicalPath()).build();
	    CommandInfo cmdInfo = CommandInfo.newBuilder().setValue("./docker_executor").addUris(uri).build();
		
		//
		// - Stick this in the executor
		//
	    this.executor = ExecutorInfo.newBuilder()
	                            .setExecutorId(ExecutorID.newBuilder().setValue(executorName))
	                            .setCommand(cmdInfo)
	                            .setName(executorName)
	                            .setSource("java")
	                            .build();
	}
	
	//
	// - Also accept pre-constructed executors
	// - IMPORTANT: note that taskImage is _not_ the executor image.
	//
	public ExecutorSpec(ExecutorInfo executorInfo, String taskImage, double reqCpu, double reqMem, int instances, JSONObject verbatim)
	{
		this.tasksLaunched = new ArrayList<TaskID>();
		this.tasksRunning = new ArrayList<TaskID>();
		
		this.taskImage = taskImage;
		
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
//	//
//	// - This will make the actors cascade-suicide
//	//	
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
	
	public void putStoppedTask(TaskID task)
	{
		this.tasksLaunched.remove(task);
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
	
	public String getTaskImage()
	{
		return this.taskImage;
	}
}
