package cn.easydat.etl.entity;

import cn.easydat.etl.entity.parameter.JobParameterReader;
import cn.easydat.etl.entity.parameter.JobParameterSetting;
import cn.easydat.etl.entity.parameter.JobParameterWriter;
import cn.easydat.etl.process.consumer.CustomTransform;
import cn.easydat.etl.process.monitor.CustomMonitor;

public class JobParameter {
	private String jobNo;
	private JobParameterReader reader;
	private JobParameterWriter writer;
	private JobParameterSetting setting;
	private CustomMonitor customMonitor;
	private CustomTransform customTransform;

	public JobParameterReader getReader() {
		return reader;
	}

	public void setReader(JobParameterReader reader) {
		this.reader = reader;
	}

	public JobParameterWriter getWriter() {
		return writer;
	}

	public void setWriter(JobParameterWriter writer) {
		this.writer = writer;
	}

	public JobParameterSetting getSetting() {
		return setting;
	}

	public void setSetting(JobParameterSetting setting) {
		this.setting = setting;
	}

	public String getJobNo() {
		return jobNo;
	}

	public void setJobNo(String jobNo) {
		this.jobNo = jobNo;
	}

	public CustomMonitor getCustomMonitor() {
		return customMonitor;
	}

	public void setCustomMonitor(CustomMonitor customMonitor) {
		this.customMonitor = customMonitor;
	}

	public CustomTransform getCustomTransform() {
		return customTransform;
	}

	public void setCustomTransform(CustomTransform customTransform) {
		this.customTransform = customTransform;
	}

	@Override
	public String toString() {
		return "JobParameter [jobNo=" + jobNo + ", reader=" + reader + ", writer=" + writer + ", setting=" + setting + "]";
	}

}
