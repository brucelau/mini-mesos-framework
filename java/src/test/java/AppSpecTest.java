import com.adsk.miniframework.AppSpec;

import java.util.Arrays;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.CommandInfo;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


@RunWith(MockitoJUnitRunner.class)
public class AppSpecTest
{	
	private final JSONParser parser = new JSONParser();
	
	private double doubleDelta;
	
	private String name;
	private double cpu;
	private double mem;
	private AppSpec spec;
	private JSONObject json;
	
	private String cmd;
	private ExecutorInfo executor;
	
	@Before
	public void setup()
	{	
		//
		// - Acceptable difference in doubles
		//
		this.doubleDelta = 0.0;

		//
		// - Constants we're using throughout our tests
		//
		this.name = "test";
		this.cpu = 1.0;
		this.mem = 16;
		this.spec = new AppSpec(this.name, this.executor, this.cpu, this.mem);
		this.json = new JSONObject();

		//
		// - This is hear because ExecutorInfo cannot be mocked... wish it could
		//
		this.cmd = "echo";
		String path = System.getProperty("user.dir");
        CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).build();
        CommandInfo executorCmd = CommandInfo.newBuilder().setValue(this.cmd).addUris(uri).build();
        
		ExecutorInfo executor = ExecutorInfo.newBuilder()
                .setExecutorId(ExecutorID.newBuilder().setValue(this.name))
                .setCommand(executorCmd)
                .setName(this.name)
                .setSource("java")
                .build();
	}
	
	@After
	public void teardown()
	{
		this.name = null;
		this.cpu = 0.0;
		this.mem = 0.0;
		this.spec = null;
		this.json = null;
		
		this.cmd = null;
		this.executor = null;
	}
	
	@Test
	public void testSpecConstruction() throws Exception
	{
		assertEquals(this.spec.jsonGetString("name"), this.name);
		assertEquals(this.spec.jsonGetDouble("required_cpus"), this.cpu, this.doubleDelta);
		assertEquals(this.spec.jsonGetDouble("required_mem"), this.mem, this.doubleDelta);
		assertEquals(this.spec.jsonGetInt("launched_tasks"), 0);
		assertEquals(this.spec.jsonGetBoolean("app_terminated"), false);
	}
	
	@Test
	public void testTaskLaunch() throws Exception
	{
		String task = "test_task_launch";
		this.spec.taskLaunched(task);

		assertEquals(Arrays.asList(task), this.spec.getTasksLaunched());
		assertEquals(1, this.spec.jsonGetInt("launched_tasks"));
	}
	
	@Test
	public void testTaskRunning() throws Exception
	{
		String task = "test_task_running";
		this.spec.taskLaunched(task);
		this.spec.taskRunning(task);
		
		assertEquals(Arrays.asList(task), this.spec.getTasksRunning());
		assertEquals(1, this.spec.jsonGetInt("running_tasks"));
	}
	
	@Test
	public void testTaskStopped() throws Exception
	{
		String task = "test_task_stopped";
		this.spec.taskLaunched(task);
		this.spec.taskRunning(task);
		this.spec.taskStopped(task);
		
		assertEquals(Arrays.asList(task), this.spec.getTasksLaunched());
		assertEquals(Arrays.asList(), this.spec.getTasksRunning());
		assertEquals(1, this.spec.jsonGetInt("launched_tasks"));
		assertEquals(0, this.spec.jsonGetInt("running_tasks"));
		assertEquals(1, this.spec.jsonGetInt("stopped_tasks"));
	}
	
	@Test
	public void testAppTerminated() throws Exception
	{
		String task = "test_task_stopped";
		this.spec.taskLaunched(task);
		this.spec.taskRunning(task);
		this.spec.taskStopped(task);
		this.spec.appTerminated();
		
		assertTrue(this.spec.jsonGetBoolean("app_terminated"));
	}
}
