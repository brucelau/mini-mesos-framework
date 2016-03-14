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

import java.io.File;
import java.util.HashMap;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

public class MiniFramework
{   
	
    private static void usage()
    {
        String name = MiniFramework.class.getName();
        System.err.println("Usage: " + name + " master ip");
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length < 1 || args.length > 2)
        {
            usage();
            System.exit(1);
        }
                
        // 
        // - TODO: read from file or poll a thread with updating specs
        // - below are just some arbitrary example specs. I.e. how many instances, cpus
        // - memory each task requires
        // 
        String[] names = {"team_a", "team_b", "team_c"};
        double[] reqCpu = {0.25, 0.25, 0.25};
        double[] reqMem = {16, 16, 32};
        String command = "./executor";

        // 
        // - This is where we store the teams' app specs
        // 
        HashMap<String, AppSpec> registered = new HashMap<String, AppSpec>();

        for(int i = 0; i < names.length; i++)
        {

            // 
            // - Have all teams using the same executor for now
            // 
            String path;
            if (System.getenv("FMWK_EXECUTOR_PATH") != null)
        	{
            	path = new File(new File(System.getenv("FMWK_EXECUTOR_PATH")), command).getCanonicalPath();
        	}
            else
            {
            	path = new File(command).getCanonicalPath();
            }
            CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).build();
            CommandInfo cmdInfo = CommandInfo.newBuilder().setValue(command).addUris(uri).build();
            
            ExecutorInfo executor = ExecutorInfo.newBuilder()
                                    .setExecutorId(ExecutorID.newBuilder().setValue("MiniExecutor_" + names[i]))
                                    .setCommand(cmdInfo)
                                    .setName("MiniExecutor_" + names[i])
                                    .setSource("java")
                                    .build();

            AppSpec spec = new AppSpec(names[i], reqCpu[i], reqMem[i], executor);

            registered.put(names[i], spec);
        }

        // 
        // - Standard framework builder used by example framework
        // 
        FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
        .setUser("") // Have Mesos fill in the current user.
        .setName("MiniFramework Java")
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
        // - standard bit of framework code
        // 
        Scheduler scheduler = new MiniScheduler(implicitAcknowledgements, registered);

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

            driver = new MesosSchedulerDriver(
                scheduler,
                frameworkBuilder.build(),
                args[0],
                implicitAcknowledgements,
                credentialBuilder.build());
        }

        // 
        // - Run the thing
        // 
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
