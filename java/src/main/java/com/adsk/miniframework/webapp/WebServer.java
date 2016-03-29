package com.adsk.miniframework.webapp;

import java.util.Collections;
import java.util.HashMap;

import com.adsk.miniframework.MiniScheduler;

import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.jboss.resteasy.test.TestPortProvider;


public class WebServer
{

	private final int port;

	public WebServer(int port)
	{
		this.port = port;
	}
	
	//
	// - Resteasy servlet running with embedded netty server
	//
	public void start(MiniScheduler scheduler) throws Exception
	{
		ResteasyDeployment deployment = new ResteasyDeployment();
		
		//
		// - Manually register handlers as singletons
		//
		deployment.setResourceClasses(Collections.singletonList(ApplicationsRest.class.getName()));
		
		//
		// - Register the running scheduler with the server
		//
		HashMap<Class, Object> objects = new HashMap<Class, Object>();
		objects.put(MiniScheduler.class, scheduler);
		deployment.setDefaultContextObjects(objects);
		
		this.start(deployment);
	}
	
	private void start(ResteasyDeployment deployment) throws Exception
	{				
		
		NettyJaxrsServer netty = new NettyJaxrsServer();
		try
		{
			netty.setDeployment(deployment);
			netty.setPort(this.port);
			netty.setRootResourcePath("");
			netty.setSecurityDomain(null);
			netty.start();
		}
		finally
		{
			// this will actually throw, so don't use it for now
			//netty.stop();
		}
	}
}