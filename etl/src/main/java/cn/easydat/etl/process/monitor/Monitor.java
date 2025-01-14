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
			int sleepSecond = 30;

			long monitorProcessRowNumLast = 0;

			// 读任务未完成或 读任务已完成且队列不为空
			while (!jobInfo.isTaskQueueFinish()) {
				long monitorProcessRowNum = 0;
				long[] monitorProcessRowNumArr = jobInfo.getMonitorProcessRowNumArr();
				for (long num : monitorProcessRowNumArr) {
					monitorProcessRowNum += num;
				}
				long processRowNum = monitorProcessRowNum - monitorProcessRowNumLast;

				float processRowNumPer = (float) (processRowNum * 1.0 / sleepSecond);

				LOG.info("平均处理行数:{},总处理行数:{}", String.format("%.2f", processRowNumPer), monitorProcessRowNum);

				if (null != jobInfo.getParameter().getCustomMonitor()) {
					MonitorInfo monitorInfo = new MonitorInfo(jobNo, null, processRowNumPer, null, monitorProcessRowNum, null);
					jobInfo.getParameter().getCustomMonitor().monitor(monitorInfo);
				}
				
				monitorProcessRowNumLast = monitorProcessRowNum;

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
