package com.adsk.miniframework;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.*;
import akka.actor.*;

public class MiniAkka
{
	//
	// - Bunch of toy messages to pass around
	// - Terminate != akka.actor.Terminated (that's a state, this is an imperative)
	//
    public static class Terminate
    {
    	private static Terminate terminate = new Terminate();
    	private Terminate(){}
    	public static Terminate getInstance()
    	{
    		return terminate;
    	}
    }
    
    public static class Initiate
    {
    	private static Initiate initiate = new Initiate();
    	private Initiate(){}
    	public static Initiate getInstance()
    	{
    		return initiate;
    	}
    }
    
    //
    // - Actor so called as its task is to just sleep
    //
	public static class Sleepy extends UntypedActor
	{
		private JSONParser parser;
		private ExecutorDriver driver;
		private TaskInfo task;
		private TaskStatus status;

		public Sleepy(final ExecutorDriver driver, final TaskInfo task)
		{
			this.driver = driver;
			this.task = task;
			this.parser = new JSONParser();		
		}

		@Override
		public void preStart()
		{
			//
			// - Let the scheduler know that the actor's started
			//
			this.status = TaskStatus.newBuilder().setTaskId(this.task.getTaskId()).setState(TaskState.TASK_RUNNING).build();
			System.out.println("Running task " + task.getTaskId().getValue() + " using actor" + this.getSelf().toString());
			this.driver.sendStatusUpdate(this.status);
			this.getSelf().tell(Initiate.getInstance(), this.getSelf());
		}
		
		@Override
		public void postStop()
		{
			//
			// - If the actor successfully exits, assume the task is finished properly
			//
			this.status = TaskStatus.newBuilder().setTaskId(this.task.getTaskId()).setState(TaskState.TASK_FINISHED).build();
			System.out.println("Stopping task " + task.getTaskId().getValue());
			this.driver.sendStatusUpdate(this.status);
		}
		
		@Override
		public void onReceive(Object message) throws Exception
		{
		
			if (message instanceof JSONObject)
			{
				JSONObject msgJson = (JSONObject) message;
				
				//
				// - Task told to stop
				//
				if (msgJson.containsKey("stop"))
				{
					//
					// - queue poisonpill; this will trigger the task_finished status update
					//
					System.out.println("JSON stop payload received");
					this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
				}

				//
				// - Catch other messages
				//
				else
				{
					unhandled(message);
				}
			}
			
			else if (message instanceof Initiate)
			{
				//
				// - Put your task here
				//
				System.out.println("Task running");
				Thread.sleep(10000);
				driver.sendFrameworkMessage(("Task running: " +  this.task.getTaskId().getValue()).getBytes());
			}
			
			else if (message instanceof Terminate)
			{
				
				//
				// - TODO gracefully shutdown
				//
				this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
			}
		}
	}
}