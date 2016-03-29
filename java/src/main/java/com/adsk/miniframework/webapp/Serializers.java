package com.adsk.miniframework.webapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.mesos.Protos.TaskID;

import com.adsk.miniframework.*;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class Serializers
{
	//
	// - Custom serializer because mesos protobufs have some self-referencing cycle in them
	//
	public static class ExecutorSpecSerializer extends JsonSerializer<ExecutorSpec>
	{
		@Override
	    public void serialize(ExecutorSpec e, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException 
		{
	        jgen.writeStartObject();
	        
	        //
	        // - This is here because ExecutorID will cause a self-referencing cycle (thanks mesos protos)
	        //
	        jgen.writeStringField("executor", e.executor.getExecutorId().getValue());
	        
	        jgen.writeNumberField("reqCpu", e.getRequiredCpu());
	        jgen.writeNumberField("reqMem", e.getRequiredMem());
	        jgen.writeNumberField("instances", e.getRequiredInstances());
	        
	        //
	        // - These are here because TaskID will cause a self-referencing cycle (thanks mesos protos)
	        //
	        List<String> tasks = new ArrayList<String>();
	        for (TaskID task : e.getLaunchedTasks())
	        {
	        	tasks.add(task.getValue());
	        }
	        jgen.writeObjectField("tasksLaunched", tasks);
	        
	        tasks = new ArrayList<String>();	        
	        for (TaskID task : e.getRunningTasks())
	        {
	        	tasks.add(task.getValue());
	        }
	        jgen.writeObjectField("tasksRunning", tasks);

	        jgen.writeEndObject();
	    }
	}
	
	//
	// - Custom serializer because mesos protobufs have some self-referencing cycle in them
	//
	public static class ApplicationSerializer extends JsonSerializer<Application>
	{
		@Override
	    public void serialize(Application a, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException 
		{
	        jgen.writeStartObject();
	    	jgen.writeStringField("name", a.name);
	    	jgen.writeNumberField("allocInstances", a.getNumRunning());
	    	jgen.writeNumberField("allocCpu", a.getCpuUsed());
	    	jgen.writeNumberField("allocMem", a.getMemUsed());
	    	jgen.writeBooleanField("appTerminated", a.getAppTerminated());
	    	jgen.writeObjectField("executors", a.getExecutors());
	        jgen.writeEndObject();
	    }
	}
}
