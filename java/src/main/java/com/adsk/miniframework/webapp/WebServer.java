package com.adsk.miniframework.webapp;

import java.util.Collections;
import java.util.HashMap;

import com.adsk.miniframework.MiniScheduler;

import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;
import org.jboss.resteasy.test.TestPortProvider;

//import com.adsk.miniframework.webapp.olds.HttpInitialiser;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

//import org.eclipse.jetty.server.*;
//import org.eclipse.jetty.server.handler.*;
//import org.eclipse.jetty.servlet.ServletContextHandler;
//import org.eclipse.jetty.servlet.ServletHolder;

public class WebServer
{

	/*
	 * Ashamedly stolen from
	 * https://nikolaygrozev.wordpress.com/2014/10/16/rest-with-embedded-jetty-
	 * and-jersey-in-a-single-jar-step-by-step/
	 */
	// public static void main(String[] args)
	// {
	//
	// Server server = new Server(8079);
	//
	//// ServletContextHandler context = new
	// ServletContextHandler(ServletContextHandler.SESSIONS);
	//// context.setContextPath("/");
	////
	//// server.setHandler(context);
	////
	//// ServletHolder servlet =
	// context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class,
	// "/*");
	//// servlet.setInitOrder(0);
	////
	//// servlet.setInitParameter("jersey.config.server.provider.classnames",
	// Applications.class.getCanonicalName());
	//
	// ContextHandler context = new ContextHandler("/applications");
	//
	//
	// }
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
	
	//
	// - Manually configured Netty http server
	//
//	public void startHttp() throws Exception
//	{
//		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
//		EventLoopGroup workerGroup = new NioEventLoopGroup();
//		try
//		{
//			ServerBootstrap bootstrap = new ServerBootstrap();
//			bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
//					.handler(new LoggingHandler(LogLevel.INFO)).childHandler(new HttpInitialiser());
//
//			//
//			// Runs the channel and binds to set port (we're using 8079)
//			// Will check for closure of channel (non blocking)
//			//
//			Channel channel = bootstrap.bind(this.port).sync().channel();
//			channel.closeFuture().sync();
//		}
//		finally
//		{
//			bossGroup.shutdownGracefully();
//			workerGroup.shutdownGracefully();
//		}
//	}
	
}

//
// import org.apache.http.client.HttpClient;
// import org.apache.http.impl.client.HttpClients;
// import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
// import org.springframework.context.annotation.*;
//
//// Stolen from ES mesos framework; s/o to those guys
//
// @EnableAutoConfiguration
// @ComponentScan
// @org.springframework.context.annotation.Configuration
// public class WebServer
// {
// @Bean
// public HttpClient httpClient() {
// return HttpClients.createSystem();
// }
// }