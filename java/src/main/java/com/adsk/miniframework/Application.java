package com.adsk.miniframework;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

import org.apache.mesos.Protos.*;
import org.apache.mesos.SchedulerDriver;
import org.json.simple.JSONObject;

public class Application
{

	//
	// - Identifier for the app
	//
	public final String name;
	
	//
	// - executors: map from executor name to ExecutorInfo for speed
	//
	private HashMap<String, ExecutorSpec> executors;
	
	//
	// - allocated resources
	//
	private int allocInstances;
	private double allocCpu;
	private double allocMem;

	//
	// - Flags for app termination
	//
	private boolean appTerminated;

	public Application(String name)
	{
		this.name = name;
		this.allocInstances = 0;
		this.allocCpu = 0;
		this.allocMem = 0;
		this.executors = new HashMap<String, ExecutorSpec>();
	}

	//
	// - Adds an executor
	//
	public void putExecutor(ExecutorInfo executorInfo, String image, double reqCpu, double reqMem, int instances, JSONObject verbatim)
	{
		ExecutorSpec executor = new ExecutorSpec(executorInfo, image, reqCpu, reqMem, instances, verbatim);
		this.executors.put(executorInfo.getName(), executor);
	}
	
	public void putExecutorSpec(ExecutorSpec executorSpec)
	{
		this.executors.put(executorSpec.executor.getName(), executorSpec);
	}
	
	//
	// - Methods to modify resource book keeping
	//
	public void putLaunchedTask(String executorName, TaskID task)
	{
		//
		// - Just keeping track of tasks launched; could use a hashtable....
		//
		this.allocInstances++;
		this.allocCpu += this.executors.get(executorName).getRequiredCpu();
		this.allocMem += this.executors.get(executorName).getRequiredMem();
		this.executors.get(executorName).putLaunchedTask(task);
	}
	
	public void putRunningTask(String executorName, TaskID task)
	{
		this.executors.get(executorName).removeLaunchedTask(task);
		this.executors.get(executorName).putRunningTask(task);
	}

	public void putStoppedTask(String executorName, TaskID task)
	{
		//
		// - Again, keeping track of tasks stopped
		//
		this.allocInstances--;
		this.allocCpu -= this.executors.get(executorName).getRequiredCpu();
		this.allocMem -= this.executors.get(executorName).getRequiredMem();
		this.executors.get(executorName).putStoppedTask(task);
	}
	
	//
	// - Set the app to terminated
	//
	public void terminateApp(SchedulerDriver driver, byte[] payload)
	{
		if (this.appTerminated)
		{
			return;
		}
		this.appTerminated = true;
		
		for (ExecutorSpec executor : this.executors.values())
		{
			executor.stopAllTasks(driver, payload);
		}
	}
	
	//
	// - Various getters for info about the apps
	//	
	public boolean getAppTerminated()
	{
		return this.appTerminated;
	}
	
	public HashMap<String, ExecutorSpec> getExecutors()
	{
		return this.executors;
	}
	
	public int getNumRunning()
	{
		int sum = 0;
		for (ExecutorSpec executor : this.executors.values())
		{
			sum += executor.getNumRunning();
		}
		return sum;
	}
	
	public int getNumLaunched()
	{
		int sum = 0;
		for (ExecutorSpec executor : this.executors.values())
		{
			sum += executor.getNumLaunched();
		}
		return sum;
	}
	
	public List<TaskID> getLaunchedTasks()
	{
		List<TaskID> tasks = new ArrayList<TaskID>();
		for (ExecutorSpec eSpec : this.executors.values())
		{
			tasks.addAll(eSpec.getLaunchedTasks());
		}
		return tasks;
	}
	
	public List<TaskID> getRunningTasks()
	{
		List<TaskID> tasks = new ArrayList<TaskID>();
		for (ExecutorSpec eSpec : this.executors.values())
		{
			tasks.addAll(eSpec.getRunningTasks());
		}
		return tasks;
	}
	
	//
	// - Get resources used
	//
	public double getCpuUsed()
	{
		double sum = 0;
		for (ExecutorSpec executor : this.executors.values())
		{
			sum += executor.getRequiredCpu() * executor.getNumRunning();
		}
		return sum;
	}
	
	public double getMemUsed()
	{
		double sum = 0;
		for (ExecutorSpec executor : this.executors.values())
		{
			sum += executor.getRequiredMem() * executor.getNumRunning();
		}
		return sum;
	}
		
	//
	// - Retrieves the entire jsonstring
	//
	public String getJsonString()
	{
		//
		// - Just return the json string
		//
		//
		JSONObject json = new JSONObject();
		json.put("name", this.name);
		json.put("allocated_instances", this.allocInstances);
		json.put("allocated_cpus", this.allocCpu);
		json.put("allocated_mem", this.allocMem);
		json.put("app_terminated", this.appTerminated);
		return json.toJSONString();
	}
}
