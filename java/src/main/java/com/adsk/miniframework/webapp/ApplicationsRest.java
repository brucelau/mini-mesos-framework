package com.adsk.miniframework.webapp;

import com.adsk.miniframework.MiniScheduler;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

@Path("/apps")
public class ApplicationsRest
{
	private final MiniScheduler scheduler;
	
	public ApplicationsRest(@Context MiniScheduler scheduler)
	{
		this.scheduler = scheduler;
	}
	
	@GET
	@Path("/")
	public Response getApps()
	{
		return Response.status(200).entity(this.scheduler.getRegisteredAppNames()).build();
	}
	
	@GET
	@Path("/{id}")
	@Produces("application/json")
	public Response getApp(@PathParam("id") String appName)
	{
		return Response.status(200).entity(this.scheduler.getRegisteredApp(appName)).build();
	}
}
