package cn.easydat.etl.entity;

public class FinishInfo {
	private String jobNo;
	private Long startTime;
	private Long endTime;
	private Long runtime;
	private Long readerRowNum;
	private Long writerRowNum;
	private Boolean success;

	public FinishInfo(String jobNo, Long startTime, Long endTime, Long runtime, Long readerRowNum, Long writerRowNum, Boolean success) {
		super();
		this.jobNo = jobNo;
		this.startTime = startTime;
		this.endTime = endTime;
		this.runtime = runtime;
		this.readerRowNum = readerRowNum;
		this.writerRowNum = writerRowNum;
		this.success = success;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public Long getRuntime() {
		return runtime;
	}

	public void setRuntime(Long runtime) {
		this.runtime = runtime;
	}

	public Long getReaderRowNum() {
		return readerRowNum;
	}

	public void setReaderRowNum(Long readerRowNum) {
		this.readerRowNum = readerRowNum;
	}

	public Long getWriterRowNum() {
		return writerRowNum;
	}

	public void setWriterRowNum(Long writerRowNum) {
		this.writerRowNum = writerRowNum;
	}

	public String getJobNo() {
		return jobNo;
	}

	public void setJobNo(String jobNo) {
		this.jobNo = jobNo;
	}

	public Boolean getSuccess() {
		return success;
	}

	public void setSuccess(Boolean success) {
		this.success = success;
	}

	@Override
	public String toString() {
		return "FinishInfo [jobNo=" + jobNo + ", startTime=" + startTime + ", endTime=" + endTime + ", runtime=" + runtime + ", readerRowNum=" + readerRowNum + ", writerRowNum=" + writerRowNum
				+ ", success=" + success + "]";
	}

}
