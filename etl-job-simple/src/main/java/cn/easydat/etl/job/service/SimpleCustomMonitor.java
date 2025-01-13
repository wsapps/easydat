package cn.easydat.etl.job.service;

import java.util.Date;

import org.noear.solon.data.sql.SqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.MonitorInfo;
import cn.easydat.etl.process.monitor.CustomMonitor;

public class SimpleCustomMonitor implements CustomMonitor {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleCustomMonitor.class);

	@Override
	public void monitor(MonitorInfo monitorInfo) {

		try {
			SqlUtils sqlUtils = SqlUtils.ofName("etlDataSource");

			sqlUtils.sql("INSERT INTO monitor_log (job_no,read_row_num_avg,write_row_num_avg,read_row_num_total,write_row_num_total,data_queue_size,monitoring_time) VALUES (?,?,?,?,?,?,?)",
					monitorInfo.getJobNo(), monitorInfo.getReadRowNumAvg(), monitorInfo.getWriteRowNumAvg(), monitorInfo.getReadRowNumTotal(), monitorInfo.getWriteRowNumTotal(),
					monitorInfo.getDataQueueSize(), new Date()).update();
		} catch (Exception e) {
			LOGGER.error("", e);
		}

	}

}
