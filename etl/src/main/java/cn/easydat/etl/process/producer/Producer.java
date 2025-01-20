package cn.easydat.etl.process.producer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.TaskNode;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;
import cn.easydat.etl.process.consumer.Consumer;
import cn.easydat.etl.process.monitor.Monitor;

public class Producer {

	private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

	private String jobNo;

	public Producer(String jobNo) {
		super();
		this.jobNo = jobNo;
	}

	public boolean startupAndAsynGen() {
		TaskNode taskNode = createReaderSplitSql();
		
		Monitor monitor = new Monitor(jobNo);
		monitor.startup();
		
		return genProducer(taskNode);
	}

	public TaskNode createReaderSplitSql() {
		JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
		JobParameter parameter = jobInfo.getParameter();
		SplitTask producer = new SplitTask();
		TaskNode taskNode = producer.split(parameter);

		jobInfo.setTaskNode(taskNode);

		return taskNode;
	}

	private boolean genProducer(TaskNode taskNode) {
		// TODO 非命令行需要先将任务入库，再取出，并标记任务状态
		JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
		boolean success = true;
		
		if (null != taskNode && null != taskNode.getReadSqlList() && taskNode.getReadSqlList().size() > 0) {
			int channel = jobInfo.getParameter().getSetting().getChannel();
			
			LOG.info("channel:{}, taskNodeNum:{}", channel, taskNode.getReadSqlList().size());
			
			ExecutorService executorService = new ThreadPoolExecutor(channel, channel, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(taskNode.getReadSqlList().size()));
			
			Future<?>[] futures = new Future[taskNode.getReadSqlList().size()];
			
			for (int i = 0; i < taskNode.getReadSqlList().size(); i++) {
				
				futures[i] = executorService.submit(new Consumer(jobNo, i));
			}
			
			for (Future<?> future : futures) {
				try {
					future.get();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					LOG.error("", e);
					success = false;
				} catch (ExecutionException e) {
					LOG.error("", e);
					success = false;
				}
			}
			
			executorService.shutdown();
			
		}

		jobInfo.setAllReaderFinish(true);
		return success;
	}

}
