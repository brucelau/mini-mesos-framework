package com.adsk.miniframework.webapp.olds;


import com.adsk.miniframework.*;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import javax.ws.rs.core.Application;
 
//public class WebApp extends Application
//{
//    private HashMap<String, Object> singletons = new HashMap();
// 
//    public WebApp(MiniScheduler scheduler) 
//    {
//        this.singletons.put("scheduler", scheduler);
//    }
// 
//    public Set<Object> getSingletons()
//    {
//        return new HashSet<Object>(this.singletons.values());
//    }
//    
//    public static void start(MiniScheduler scheduler)
//    {
//    	
//        WebServer server = new WebServer(8079);
//        try {
//        	
//        	
//
//            WebAppContext webAppContext = new WebAppContext();
//            webAppContext.setContextPath("/");
//            webAppContext.setWar(WAR_LOCATION);
//
//            webAppContext.addEventListener(new org.jboss.resteasy.plugins.server.servlet.ResteasyBootstrap(){
//                @Override
//                public void contextInitialized(ServletContextEvent event) {
//                    super.contextInitialized(event);
//                    deployment.getDispatcher().getDefaultContextObjects().put(RecordManager.class, recman);
//                }
//            });
//
//            webAppContext.addServlet(org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher.class, "/*");
//
//            webAppContext.setServer(server);
//            server.setHandler(webAppContext);
//
//            server.start();
//
//        } catch (Exception e) {
//            logger.error("Error when starting", e);
//            server.stop();
//        } 
//    }
//}