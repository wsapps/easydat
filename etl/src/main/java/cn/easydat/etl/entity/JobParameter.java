package cn.easydat.etl.entity;

import cn.easydat.etl.entity.parameter.JobParameterReader;
import cn.easydat.etl.entity.parameter.JobParameterSetting;
import cn.easydat.etl.entity.parameter.JobParameterWriter;

public class JobParameter {
	private JobParameterReader reader;
	private JobParameterWriter writer;
	private JobParameterSetting setting;

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

	@Override
	public String toString() {
		return "JobParameter [reader=" + reader + ", writer=" + writer + ", setting=" + setting + "]";
	}

}
