package cn.easydat.etl.process.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.MonitorInfo;
import cn.easydat.etl.process.JobContainer;
import cn.easydat.etl.process.JobInfo;

public class Monitor {

	private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);

	private String jobNo;

	public Monitor(String jobNo) {
		super();
		this.jobNo = jobNo;
	}

	public void startup() {
		Thread thread = new Thread(() -> {
			JobInfo jobInfo = JobContainer.JOB_MAP.get(jobNo);
			int sleepSecond = 15;

			long monitorReaderRowNumLast = 0;
			long monitorWriterRowNumLast = 0;

			// 读任务未完成或 读任务已完成且队列不为空
			while (!jobInfo.isTaskQueueFinish() || (jobInfo.isAllReaderFinish() && !jobInfo.dataQueueIsEmpty())) {
				long monitorReaderRowNum = jobInfo.monitorReaderRowNumGet();
				long monitorWriterRowNum = jobInfo.monitorWriterRowNumGet();

				long readerRowNum = monitorReaderRowNum - monitorReaderRowNumLast;
				long writerRowNum = monitorWriterRowNum - monitorWriterRowNumLast;

				float readerRowNumPer = (float) (readerRowNum * 1.0 / sleepSecond);
				float writerRowNumPer = (float) (writerRowNum * 1.0 / sleepSecond);

				LOG.info("平均读取行数:{},平均写入行数:{},读取总行数:{}, 写入总行数:{},QueueSize:{}", String.format("%.2f", readerRowNumPer), String.format("%.2f", writerRowNumPer), monitorReaderRowNum, monitorWriterRowNum,jobInfo.dataQueueSize());

				if (null != jobInfo.getParameter().getCustomMonitor()) {
					MonitorInfo monitorInfo = new MonitorInfo(jobNo, readerRowNumPer, writerRowNumPer, monitorReaderRowNum, monitorWriterRowNum, jobInfo.dataQueueSize());
					jobInfo.getParameter().getCustomMonitor().monitor(monitorInfo);
				}
				
				monitorReaderRowNumLast = monitorReaderRowNum;
				monitorWriterRowNumLast = monitorWriterRowNum;

				try {
					Thread.sleep(sleepSecond * 1000);
				} catch (InterruptedException e) {
				}
			}
		});
		thread.setName("Monitor-" + jobNo);
		thread.start();
	}
}
