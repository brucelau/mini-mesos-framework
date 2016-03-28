package com.adsk.miniframework.webapp.olds;

//import com.adsk.miniframework.MiniScheduler;
//import com.adsk.miniframework.Application;
//import com.adsk.miniframework.ExecutorSpec;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.RequestMethod;
//import org.springframework.web.bind.annotation.RequestBody;
//import org.springframework.web.bind.annotation.RestController;
//
//import org.json.simple.JSONObject;
//
//@RestController
//public class ApplicationController
//{
//	@Autowired
//	MiniScheduler scheduler;
//	
//	//
//	// - Handler for just getting the list of running apps
//	// - Includes wrapper for getting app names
//	//
//	public class AppListResponse
//	{
//		private List<String> names;
//		
//		public AppListResponse(List<String> names)
//		{
//			this.names = names;
//		}
//	}
//	
//	@RequestMapping(value = "/apps", method = RequestMethod.GET)
//	public ResponseEntity<AppListResponse> getApps()
//	{
//		AppListResponse resp = new AppListResponse(this.scheduler.getRegisteredAppNames());
//		return new ResponseEntity<AppListResponse>(resp, HttpStatus.OK);
//	}
//	
//	//
//	// - Handler for posting new application specification
//	// - Includes wrapper for creating apps and executor
//	//
//	public class AppWrapper
//	{
//		private String name;
//		private List<ExecWrapper> executors;
//		
//		public AppWrapper(String name, List<ExecWrapper> executors)
//		{
//			this.name = name;
//			this.executors = executors;
//		}
//		
//		public Application buildApp() throws Exception
//		{
//			Application app = new Application(this.name);
//			for (ExecWrapper executor : this.executors)
//			{
//				ExecutorSpec exec = executor.buildExecutorSpec();
//				app.putExecutorSpec(exec);
//			}
//			return app;
//		}
//	}
//	
//	public class ExecWrapper
//	{
//		private String name;
//		private String image;
//		private String command;
//		private boolean forcePull;
//		private double cpus;
//		private double mem;
//		private int instances;
//		private JSONObject verbatim;
//		
//		public ExecWrapper(String name, String image, String command, boolean forcePull, double cpus, double mem, int instances)
//		{
//			this(name, image, command, forcePull, cpus, mem, instances, new JSONObject());
//		}
//		
//		public ExecWrapper(String name, String image, String command, boolean forcePull, double cpus, double mem, int instances, JSONObject verbatim)
//		{
//			this.name = name;
//			this.image = image;
//			this.command = command;
//			this.forcePull = forcePull;
//			this.cpus = cpus;
//			this.mem = mem;
//			this.instances = instances;
//			this.verbatim = verbatim;
//		}
//		
//		public ExecutorSpec buildExecutorSpec() throws Exception
//		{
//			return new ExecutorSpec(this.name, this.image, this.command, this.forcePull, this.cpus, this.mem, this.instances, this.verbatim);
//		}
//	}
//	
//	@RequestMapping(value = "/apps/create", method = RequestMethod.POST, consumes="application/json")
//	public ResponseEntity createApp(@RequestBody AppWrapper appWrap)
//	{
//		if (appWrap != null)
//		{
//			System.out.println("received REST POST for: " );
//			try
//			{
//				Application app = appWrap.buildApp();
//				this.scheduler.registerApp(app);
//				return new ResponseEntity<AppWrapper>(appWrap, HttpStatus.OK);
//			}
//			
//			catch(Exception e)
//			{
//				System.out.println("couldn't process REST POST: " + e);
//				return new ResponseEntity<String>("Error: " + e, HttpStatus.BAD_REQUEST);
//			}
//		}
//		return new ResponseEntity<String>("Error: found no application specification.",HttpStatus.UNPROCESSABLE_ENTITY); 
//	}
//}
