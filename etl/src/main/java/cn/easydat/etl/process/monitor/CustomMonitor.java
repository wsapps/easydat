package cn.easydat.etl.process.monitor;

import cn.easydat.etl.entity.MonitorInfo;

public interface CustomMonitor {

	void monitor(MonitorInfo monitorInfo);
}
