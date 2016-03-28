package com.adsk.miniframework;

//import com.adsk.miniframework.server.WebApp;
//import org.springframework.boot.builder.SpringApplicationBuilder;

import java.util.UUID;
import java.util.HashMap;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

import com.adsk.miniframework.webapp.WebServer;

public class MiniFramework
{   
	
    private static void usage()
    {
        String name = MiniFramework.class.getName();
        System.err.println("Usage: " + name + " master ip");
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length < 1 )
        {
            usage();
            System.exit(1);
        }
                
        // 
        // - Standard framework builder used by example framework
        // 
        
        FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("MiniFramework Java")
        .setId(FrameworkID.newBuilder().setValue("MiniFramework-" + UUID.randomUUID()).build())
        .setCheckpoint(true);

        // 
        // - Explicit status/registration change acknowledgements
        // 
        boolean implicitAcknowledgements = true;

        if (System.getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS") != null)
        {
            System.out.println("Enabling explicit acknowledgements for status updates");
            implicitAcknowledgements = false;
        }

        // 
        // - build the scheduler; look out for credentials
        //
        Scheduler scheduler = new MiniScheduler(implicitAcknowledgements);
        
        //
        // - standard bit of framework code to look for credentials
        // 
        MesosSchedulerDriver driver = null;

        if (System.getenv("AUTHENTICATE") == null)
        {
            frameworkBuilder.setPrincipal("");

            driver = new MesosSchedulerDriver(scheduler, 
            		frameworkBuilder.build(), 
            		args[0], 
            		implicitAcknowledgements);
        }

        else
        {
            System.out.println("Enabling authentication for the framework");

            if (System.getenv("PRINCIPAL") == null)
            {
                System.err.println("Expecting authentication principal in the environment");
                System.exit(1);
            }

            Credential.Builder credentialBuilder = Credential.newBuilder()
                                                   .setPrincipal(System.getenv("PRINCIPAL"));

            if (System.getenv("SECRET") != null)
            {
                credentialBuilder.setSecret(System.getenv("SECRET"));
            }

            frameworkBuilder.setPrincipal(System.getenv("PRINCIPAL"));

            driver = new MesosSchedulerDriver(scheduler,
                frameworkBuilder.build(),
                args[0],
                implicitAcknowledgements,
                credentialBuilder.build());
        }
        
        //
        // - New: run parallel http for rest api
        //
        System.out.println("Building API...");
        
//        HashMap<String, Object> properties = new HashMap<>();
//        properties.put("server.port", String.valueOf(8079));
//        new SpringApplicationBuilder(WebApp.class)
//	        .properties(properties)
//	        .initializers(applicationContext -> applicationContext.getBeanFactory().registerSingleton("scheduler", scheduler))
//	        .run(args);
        WebServer server = new WebServer(8079);
        server.start((MiniScheduler) scheduler);
        
        System.out.println("Netty server launched.");
        
        // 
        // - Run the thing
        // 
        System.out.println("Running scheduler driver...");
        int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;
        
        // Ensure that the driver process terminates.
        driver.stop();

        // For this test to pass reliably on some platforms, this sleep is
        // required to ensure that the SchedulerDriver teardown is complete
        // before the JVM starts running native object destructors after
        // System.exit() is called. 500ms proved successful in test runs,
        // but on a heavily loaded machine it might not.
        // TODO(greg): Ideally, we would inspect the status of the driver
        // and its associated tasks via the Java API and wait until their
        // teardown is complete to exit.
        Thread.sleep(500);

        System.exit(status);
    }
}
