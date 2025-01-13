package cn.easydat.etl.process.consumer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;

public class Consumer {

	private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

	private String jobNo;

	public Consumer(String jobNo) {
		super();

		this.jobNo = jobNo;
	}

	public void startup() {
		JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
		
		// 读任务未完成或队列不为空
		if(!jobInfo.isAllReaderFinish() || !jobInfo.dataQueueIsEmpty()) {
			JobParameter parameter = jobInfo.getParameter();
			int channel = parameter.getSetting().getChannel();
			ExecutorService executorService = new ThreadPoolExecutor(channel, channel, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(channel));
			
			LOG.info("startup channel:" + channel);
			
			for (int i = 0; i < channel; i++) {
				executorService.submit(new WriterTask(jobNo, i));
			}
			
			executorService.shutdown();
			
			while (!executorService.isTerminated()) {
				try {
					executorService.awaitTermination(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.error("", e);
				}
			}
			
			LOG.info("Reader finish, jobNo:" + jobNo);
		}
	}

}
