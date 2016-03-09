package com.adsk.miniframework;

import java.util.ArrayList;
import java.util.List;

import org.apache.mesos.Protos.ExecutorInfo;
import org.json.simple.JSONObject;

public class AppSpec {
	
	//
	// - Identifier for the app
    //
	public final String name;
    
    // 
    // - Executor info specced by the team
    // - assume for now that communication is handled for us
    // - otherwise, will have to add callback in this class
    // - run when status update is from a particular app
    // 
    public ExecutorInfo executor;

    // 
    // - required resources per task
    // 
    private double reqCpu;
    private double reqMem;

    // 
    // - allocated resources
    // 
    private int allocInstances;
    private double allocCpu;
    private double allocMem; 
        
	//        
	// - Use JSONObject instead     
    // - Should be easier to pass around threads as data
	//        
    private JSONObject json;
    
    //
    // - Terminated application
    //
    private boolean appTerminated;
    
    //
    // - Tasks
    //
    private List<String> tasksLaunched;
    private List<String> tasksRunning;
    
    public AppSpec(String name, ExecutorInfo executor, double reqCpu, double reqMem)
    {
        this.name = name;
        this.executor = executor;

        this.reqCpu = reqCpu;
        this.reqMem = reqMem;

        this.allocInstances = 0;
        this.allocCpu = 0;
        this.allocMem = 0;

        this.tasksLaunched = new ArrayList<String>();
        this.tasksRunning = new ArrayList<String>();
        
        //
        // - Make an empty json using jsonUpdate()
        //
        this.json = new JSONObject();
        this.jsonUpdate();
    }
   
    private void jsonUpdate()
    {
    	//
    	// - Just put new specs into the json
    	//
    	this.json.put("name", this.name);
	   	this.json.put("required_cpus", this.reqCpu);
	   	this.json.put("required_mem", this.reqMem);
	   	this.json.put("allocated_instances", this.allocInstances);
	   	this.json.put("allocated_cpus", this.allocCpu);
	   	this.json.put("allocated_mem", this.allocMem);
	   	this.json.put("launched_tasks", this.tasksLaunched.size());
	   	this.json.put("running_tasks", this.tasksRunning.size());
	   	this.json.put("stopped_tasks", this.tasksLaunched.size() - this.tasksRunning.size());
	   	this.json.put("app_terminated", this.appTerminated);
    }
    
    //
    // - Methods to modify resource book keeping
    //
    public void taskLaunched(String task)
    {
        // 
        // - Just keeping track of tasks launched; could use a hashtable....
        // 
        this.allocInstances++;
        this.allocCpu += this.reqCpu;
        this.allocMem += this.reqMem;
        this.tasksLaunched.add(task);
        this.jsonUpdate();
    }
    
    public void tasksRunning(String task)
    {
    	this.tasksRunning.add(task);
    	this.jsonUpdate();
    }

    public void taskStopped(String task)
    {
        // 
        // - Again, keeping track of tasks stopped
        // 
        this.allocInstances--;
        this.allocCpu -= this.reqCpu;
        this.allocMem -= this.reqMem;
        this.tasksRunning.remove(task);
        this.jsonUpdate();
    }
    
    //
    // - Set the app to terminated
    //
    public void appTerminated()
    {
    	this.appTerminated = true;
    	this.jsonUpdate();
    }
    
    //
    // - Retrieves the entire jsonstring
    //
    public String jsonString()
    {
    	//
    	// - Just return the json string
    	//
    	return this.json.toJSONString();
    }
    
    //
    // - Various getters for info about the apps
    //
    public String jsonGetString(String key)
    {
    	return this.json.get(key).toString();
    }
    
    public int jsonGetInt(String key)
    {
    	return Integer.parseInt(this.jsonGetString(key));
    }
    
    public double jsonGetDouble(String key)
    {
    	return Double.parseDouble(this.jsonGetString(key));
    }
    
    public boolean jsonGetBoolean(String key)
    {
    	return Boolean.parseBoolean(this.jsonGetString(key));
    }
    
    public List<String> getTasksLaunched()
    {
    	return this.tasksLaunched;
    }
    
    public List<String> getTasksRunning()
    {
    	return this.tasksRunning;
    }
    
}
