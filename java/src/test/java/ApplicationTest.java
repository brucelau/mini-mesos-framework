import com.adsk.miniframework.Application;
import com.adsk.miniframework.MiniScheduler;

import java.util.Arrays;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import org.apache.mesos.Protos.*;
import org.apache.mesos.SchedulerDriver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(MockitoJUnitRunner.class)
public class ApplicationTest
{	
	ObjectMapper mapper = MiniScheduler.getObjectMapper();
	private double doubleDelta;
	
	private String name;
	private double cpu;
	private double mem;
	private JsonNode json;
	private String cmd;
	private String image;
	
	private Application spec;
	private ExecutorInfo executorInfo;
	private TaskInfo task;
	
	@Mock
	private SchedulerDriver driver;
	
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
		this.spec = new Application(this.name);
		this.json = mapper.createObjectNode();

		//
		// - This is here because ExecutorInfo cannot be mocked... wish it could
		//
		this.cmd = "echo";
		this.image = "ubuntu:14.04";
				
		String path = System.getProperty("user.dir");
        CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).build();
        CommandInfo executorCmd = CommandInfo.newBuilder().setValue(this.cmd).addUris(uri).build();
        
		this.executorInfo = ExecutorInfo.newBuilder()
                .setExecutorId(ExecutorID.newBuilder().setValue(this.name + "-executor"))
                .setCommand(executorCmd)
                .setName(this.name + "-executor")
                .setSource("java")
                .build();
		
		this.spec.putExecutor(this.executorInfo, this.image, this.cpu, this.mem, 1, this.json);
		
		//
		// - Same for the task
		//

        // - Set task ID as name + number launched 
        // 
        TaskID taskID = TaskID.newBuilder().setValue(this.name + "-task").build();
                      
        // 
        // - Queue the task onto the launcher
        // 
        this.task = TaskInfo.newBuilder()
                        .setName("task " + taskID.getValue())
                        .setTaskId(taskID)
                        // Get the required cpus off of the specs
                        .addResources(Resource.newBuilder()
                                      .setName("cpus")
                                      .setType(Value.Type.SCALAR)
                                      .setScalar(Value.Scalar.newBuilder().setValue(this.cpu)))
                        // Do the same for required mem
                        .addResources(Resource.newBuilder()
                                      .setName("mem")
                                      .setType(Value.Type.SCALAR)
                                      .setScalar(Value.Scalar.newBuilder().setValue(this.mem)))
                        .setExecutor(ExecutorInfo.newBuilder(this.executorInfo))
                        .setSlaveId(SlaveID.newBuilder().setValue(this.name + "-slave"))
                        .build();
	}
	
	@After
	public void teardown()
	{
		//		
		// - Reset our test objects
		//
		this.name = null;
		this.cpu = 0.0;
		this.mem = 0.0;
		this.spec = null;
		this.json = null;
		
		this.cmd = null;
		this.executorInfo = null;
	}
	
	@Test
	public void testSpecConstruction()
	{
		//
		// - Tests that a spec is initialised properly
		//
		assertEquals(this.spec.name, this.name);
		assertEquals(this.spec.getNumLaunched(), 0);
		assertEquals(this.spec.getAppTerminated(), false);
	}
	
	@Test
	public void testTaskLaunch()
	{
		//
		// - Tests that tasks are launched correctly
		//
		this.spec.putLaunchedTask(this.name + "-executor", this.task.getTaskId());

		assertEquals(Arrays.asList(this.task.getTaskId()), this.spec.getLaunchedTasks());
		assertEquals(1, this.spec.getNumLaunched());
	}
	
	@Test
	public void testTaskRunning() throws Exception
	{
		//
		// - Tests that logic with launched and running tasks is correct
		// - Launched tasks should have one task after launch
		//
		this.spec.putLaunchedTask(this.executorInfo.getExecutorId().getValue(), this.task.getTaskId());
		assertEquals(Arrays.asList(task.getTaskId()), this.spec.getLaunchedTasks());
		
		//
		// - When task is running, launched tasks should be empty
		//
		this.spec.putRunningTask(this.executorInfo.getExecutorId().getValue(), this.task.getTaskId());
		assertEquals(Arrays.asList(), this.spec.getLaunchedTasks());
		assertEquals(Arrays.asList(task.getTaskId()), this.spec.getRunningTasks());
		assertEquals(0, this.spec.getNumLaunched());
		assertEquals(1, this.spec.getNumRunning());
	}
	
	@Test
	public void testTaskStopped() throws Exception
	{
		//
		// - Tests that logic with stopped tasks is corect
		//
		this.spec.putLaunchedTask(this.executorInfo.getExecutorId().getValue(), this.task.getTaskId());
		this.spec.putRunningTask(this.executorInfo.getExecutorId().getValue(), this.task.getTaskId());
		this.spec.putStoppedTask(this.executorInfo.getExecutorId().getValue(), this.task.getTaskId());
		
		assertEquals(Arrays.asList(), this.spec.getLaunchedTasks());
		assertEquals(Arrays.asList(), this.spec.getRunningTasks());
		assertEquals(0, this.spec.getNumRunning());
	}
	
	@Test
	public void testAppTerminated() throws Exception
	{
		//
		// - Makes sure that apps can be terminated properly
		//
		String task = "test_task_stopped";
		this.spec.putLaunchedTask(this.executorInfo.getExecutorId().getValue(), this.task.getTaskId());
		this.spec.putRunningTask(this.executorInfo.getExecutorId().getValue(), this.task.getTaskId());
		this.spec.putStoppedTask(this.executorInfo.getExecutorId().getValue(), this.task.getTaskId());
		this.spec.terminateApp(this.driver, new byte[0]);
		
		assertTrue(this.spec.getAppTerminated());
	}
}
