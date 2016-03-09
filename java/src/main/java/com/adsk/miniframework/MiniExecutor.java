/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adsk.miniframework;

import java.util.HashMap;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.ActorSystem;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class MiniExecutor implements Executor
{

	private JSONParser parser;
	private HashMap<String, ActorRef> actors;
	private final ActorSystem system = ActorSystem.create("ExecutorSystem");
	private TaskInfo task;
	
	public MiniExecutor()
	{
		this.parser = new JSONParser();
		this.actors = new HashMap<String, ActorRef>();
	}
	
	@Override
	public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo,
			SlaveInfo slaveInfo)
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
	}

	@Override
	public void launchTask(final ExecutorDriver driver, final TaskInfo task)
	{
		this.actors.put(task.getTaskId().getValue(),
				this.system.actorOf(Props.create(MiniAkka.Sleepy.class, driver, task), "sleepy-" + task.getTaskId().getValue()));	
	}

	@Override
	public void killTask(ExecutorDriver driver, TaskID taskId)
	{
	}

	@Override
	public void frameworkMessage(ExecutorDriver driver, byte[] data)
	{
		String message = new String(data);
		JSONObject msgJson = new JSONObject();

		try
		{
			msgJson = (JSONObject) this.parser.parse((String) message);
			this.actors.get(msgJson.get("task")).tell(msgJson, ActorRef.noSender());
		}
		catch (ParseException e)
		{
			//this.actors.get(msgJson.get("task")).tell(message, ActorRef.noSender());
		}
	}

	@Override
	public void shutdown(ExecutorDriver driver)
	{
	}

	@Override
	public void error(ExecutorDriver driver, String message)
	{
	}

	public static void main(String[] args) throws Exception
	{
		System.out.println("Executor initialising...");
		MesosExecutorDriver driver = new MesosExecutorDriver(new MiniExecutor());
		System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
	}
}
