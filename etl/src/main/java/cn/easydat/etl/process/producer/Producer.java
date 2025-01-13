package cn.easydat.etl.process.producer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.TaskInfo;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;

public class Producer {

	private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

	private String jobNo;

	public Producer(String jobNo) {
		super();
		this.jobNo = jobNo;
	}

	public void startupAndAsynGen() {
		Map<String, List<String>> sqlsMap = createReaderSplitSql();

		Thread readerThread = new Thread(() -> {
			genProducer(sqlsMap);
		});
		readerThread.setName("readerMain");

		readerThread.start();
	}

	public Map<String, List<String>> createReaderSplitSql() {
		JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
		JobParameter parameter = jobInfo.getParameter();
		SplitTask producer = new SplitTask();
		Map<String, List<String>> sqlsMap = producer.split(parameter);

//		LOG.info("readerSplitSize size:" + (null != readerSplitSqlList ? readerSplitSqlList.size() : "null"));
		
		jobInfo.setSqlsMap(sqlsMap);

		return sqlsMap;
	}

	private void genProducer(Map<String, List<String>> sqlsMap) {
		// TODO 非命令行需要先将任务入库，再取出，并标记任务状态
		JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
		
		if (null != sqlsMap && null != sqlsMap.get("select") && sqlsMap.get("select").size() > 0) {
			int channel = jobInfo.getParameter().getSetting().getChannel();
			
			ExecutorService executorService = new ThreadPoolExecutor(channel, channel, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(sqlsMap.get("select").size()));
			
			for (int i = 0; i < sqlsMap.get("select").size(); i++) {
				TaskInfo taskInfo = new TaskInfo(i, sqlsMap.get("select").get(i), sqlsMap.get("delete").get(i));
				executorService.submit(new ReaderTask(jobNo, i, taskInfo));
			}
			
			executorService.shutdown();
			
			while (!executorService.isTerminated()) {
				try {
					executorService.awaitTermination(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.error("", e);
				}
			}
		}

		jobInfo.setAllReaderFinish(true);
	}

}
