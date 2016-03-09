package com.adsk.miniframework;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.*;
import akka.actor.*;

public class MiniAkka
{

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
		}
		
		@Override
		public void postStop()
		{
			this.status = TaskStatus.newBuilder().setTaskId(this.task.getTaskId()).setState(TaskState.TASK_FINISHED).build();
			System.out.println("Stopping task " + task.getTaskId().getValue());
			this.driver.sendStatusUpdate(this.status);
		}
		
		@Override
		public void onReceive(Object message)
		{
		
			JSONObject msgJson = (JSONObject) message;
			
			//
			// - Task told to stop
			//
			if (msgJson.containsKey("stop"))
			{
				//
				// - update status and let the framework know
				// - Then queue poisonpill
				//
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
	}
}