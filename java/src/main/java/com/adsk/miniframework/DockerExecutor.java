package com.adsk.miniframework;

import java.util.HashMap;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.ActorSystem;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class DockerExecutor implements Executor
{
	//
	// - If app has an executor, want tasks to be launched with accompanying actor
	//
	private JSONParser parser;
	private HashMap<String, ActorRef> actors;
	private final ActorSystem system = ActorSystem.create("ExecutorSystem");
	
	public DockerExecutor()
	{
		this.parser = new JSONParser();
		this.actors = new HashMap<String, ActorRef>();
	}
	
	@Override
	public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo)
	{
		System.out.println("Registered executor on " + slaveInfo.getHostname());
	}

	@Override
	public void reregistered(ExecutorDriver driver, SlaveInfo executorInfo)
	{
	}

	@Override
	public void disconnected(ExecutorDriver driver)
	{
		System.out.println("Executor disconnected");
	}

	@Override
	public void launchTask(final ExecutorDriver driver, final TaskInfo task)
	{
		this.actors.put(task.getTaskId().getValue(),
				this.system.actorOf(Props.create(MiniAkka.Sleepy.class, driver, task), task.getTaskId().getValue() + "-actor"));	
	}

	@Override
	public void killTask(ExecutorDriver driver, TaskID taskId)
	{
		System.out.println("Task " + taskId.getValue() + " killed.");
		driver.sendFrameworkMessage(("Task " + taskId.getValue() + " killed.").getBytes());
	}

	@Override
	public void frameworkMessage(ExecutorDriver driver, byte[] data)
	{
		String message = new String(data);
		JSONObject msgJson = new JSONObject();

		try
		{
			msgJson = (JSONObject) this.parser.parse((String) message);
//			this.actors.get(msgJson.get("task")).tell(msgJson, ActorRef.noSender());
		}
		catch (Exception e)
		{
			// Only interested in JSON serialisable messages for now
		}
	}

	@Override
	public void shutdown(ExecutorDriver driver)
	{
	}

	@Override
	public void error(ExecutorDriver driver, String message)
	{
		System.out.println("Executor error: " + message);
		driver.sendFrameworkMessage(("Executor Error: " + message).getBytes());
	}
	
	public static void main(String[] args) throws Exception
	{	
		try
		{
			System.out.println("Executor initialising...");
			MesosExecutorDriver driver = new MesosExecutorDriver(new DockerExecutor());
			System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
			System.out.println("Executor terminated.");
		}
		catch (Exception e)
		{
			System.out.println("Executor exception: " + e);
			System.exit(1);
		}
	}
}
