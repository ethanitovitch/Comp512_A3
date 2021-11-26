import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.nio.charset.StandardCharsets;

public class DistProcess implements Watcher
{
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isMaster=false;
	private volatile boolean connected = false;
	private volatile boolean expired = false;

	DistProcess(String zkhost)
	{
		zkServer=zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

	public void process(WatchedEvent e) {
	}

	// Try to become the master.
	void runForMaster() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist23/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	void createWorker() throws UnknownHostException, KeeperException, InterruptedException {
		zk.create("/dist23/workers/" + pinfo, "Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		zk.create("/dist23/assigned/" + pinfo, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 1000, this); //connect to ZK.
		try
		{
			runForMaster();	// See if you can become the master (i.e, no other master exists)
			isMaster=true;
			getTasks(); // Install monitoring on any new tasks that will be created.

		} catch(NodeExistsException nee)
		{
			isMaster=false;
			createWorker();
			getWorkerTasks();
		}

		System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isMaster?"master":"worker"));
	}

	Watcher tasksChangeWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if(e.getType() == EventType.NodeChildrenChanged && e.getPath().equals("/dist23/tasks")) {
				getTasks();
			}
		}
	};

	void getTasks()
	{
		zk.getChildren("/dist23/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
	}

	ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx, List<String> children){
			for (String child : children) {
				getData(path + "/" + child, child);
			}
		}
	};

	void getData(String path, String task) {
		zk.getData(path,
				false,
				getDataCallback,
				task);
	}

	class TaskCtx {
		String path;
		String task;
		byte[] data;

		String workerPath = null;
		String worker = null;
		TaskCtx(String path, String task, byte[] data) {
			this.path = path;
			this.task = task;
			this.data = data;
		}

		void setWorkerPath(String workerPath) {
			this.workerPath = workerPath;
			this.worker = workerPath.split("/")[workerPath.split("/").length-1];
		}
	}

	DataCallback getDataCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
			getIdleWorker(new TaskCtx(path, (String) ctx, data));
		}
	};

	Watcher idleWorkerWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if(e.getType() == EventType.NodeChildrenChanged && e.getPath().equals("/dist23/workers")) {
				getTasks();
			}
		}
	};

	void getIdleWorker(TaskCtx taskCtx) {
		zk.getChildren("/dist23/workers", idleWorkerWatcher, workersGetChildrenCallback, taskCtx);
	}

	ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx, List<String> children){
			for (String child : children) {
				getWorkerData(path + "/" + child, (TaskCtx)ctx);
			}
		}
	};

	void getWorkerData(String path, TaskCtx ctx) {
		zk.getData(path, false, getWorkerDataCallback, ctx);
	}

	DataCallback getWorkerDataCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
			String s = new String(data, StandardCharsets.UTF_8);

			TaskCtx task = (TaskCtx) ctx;
			if (s.equals("Idle") && task.workerPath == null) {
				task.setWorkerPath(path);
				recreateTask(task);
			}
		}
	};

	void recreateTask(TaskCtx ctx) {
		zk.create("/dist23/assigned/" + ctx.worker + "/" + ctx.task,
				ctx.data,
				Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL,
				recreateTaskCallback,
				ctx);
	}

	StringCallback recreateTaskCallback = new StringCallback() {
		public void processResult(int rc, String path, Object ctx, String name) {
			setWorkerWorking((TaskCtx) ctx);
		}
	};

	void setWorkerWorking(TaskCtx ctx) {
		zk.setData(ctx.workerPath, "Working".getBytes(), -1, null, ctx);
	}

	Watcher tasksAssignedWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if (e.getType() == EventType.NodeChildrenChanged && e.getPath().equals("/dist23/assigned/" + pinfo)) {
				getWorkerTasks();
			}
		}
	};

	void getWorkerTasks()
	{
		zk.getChildren("/dist23/assigned/" + pinfo, tasksAssignedWatcher, processTaskCallback, null);
	}

	ChildrenCallback processTaskCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			for (String child : children) {
				getTaskData(path + "/" + child, child);
			}
		}
	};

	void getTaskData(String path, String task) {
		zk.getData(path, null, handleTaskDataCallback, task);
	}

	DataCallback handleTaskDataCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
			handleTaskData(data, (String) ctx, path);
		}
	};

	void handleTaskData(byte[] taskSerial, String task, String path) {
		// Re-construct our task object.
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
			ObjectInput in = new ObjectInputStream(bis);
			DistTask dt = (DistTask) in.readObject();
			//Execute the task.
			dt.compute();

			// Serialize our Task object back to a byte array!
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(dt); oos.flush();
			taskSerial = bos.toByteArray();

			// Store it inside the result node.
			zk.delete(path, -1);
			zk.create("/dist23/tasks/"+task+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.setData("/dist23/workers/" + pinfo, "Idle".getBytes(), -1);
			//zk.create("/dist23/tasks/"+c+"/result", ("Hello from "+pinfo).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		catch(NodeExistsException nee){System.out.println(nee);}
		catch(KeeperException ke){System.out.println(ke);}
		catch(InterruptedException ie){System.out.println(ie);}
		catch(IOException io){System.out.println(io);}
		catch(ClassNotFoundException cne){System.out.println(cne);}
	}

	public static void main(String args[]) throws Exception
	{
		//Create a new process
		//Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		//Replace this with an approach that will make sure that the process is up and running forever.
		synchronized(dt)
		{
			try { dt.wait(); }
			catch(InterruptedException ie){}
		}
	}
}
