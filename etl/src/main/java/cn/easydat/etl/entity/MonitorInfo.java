package cn.easydat.etl.entity;

public class MonitorInfo {
	private String jobNo;
	private Float readRowNumAvg;
	private Float writeRowNumAvg;
	private Long readRowNumTotal;
	private Long writeRowNumTotal;
	private Integer dataQueueSize;

	public MonitorInfo() {
		super();
	}

	public MonitorInfo(String jobNo, Float readRowNumAvg, Float writeRowNumAvg, Long readRowNumTotal, Long writeRowNumTotal, Integer dataQueueSize) {
		super();
		this.jobNo = jobNo;
		this.readRowNumAvg = readRowNumAvg;
		this.writeRowNumAvg = writeRowNumAvg;
		this.readRowNumTotal = readRowNumTotal;
		this.writeRowNumTotal = writeRowNumTotal;
		this.dataQueueSize = dataQueueSize;
	}

	public String getJobNo() {
		return jobNo;
	}

	public void setJobNo(String jobNo) {
		this.jobNo = jobNo;
	}

	public Float getReadRowNumAvg() {
		return readRowNumAvg;
	}

	public void setReadRowNumAvg(Float readRowNumAvg) {
		this.readRowNumAvg = readRowNumAvg;
	}

	public Float getWriteRowNumAvg() {
		return writeRowNumAvg;
	}

	public void setWriteRowNumAvg(Float writeRowNumAvg) {
		this.writeRowNumAvg = writeRowNumAvg;
	}

	public Long getReadRowNumTotal() {
		return readRowNumTotal;
	}

	public void setReadRowNumTotal(Long readRowNumTotal) {
		this.readRowNumTotal = readRowNumTotal;
	}

	public Long getWriteRowNumTotal() {
		return writeRowNumTotal;
	}

	public void setWriteRowNumTotal(Long writeRowNumTotal) {
		this.writeRowNumTotal = writeRowNumTotal;
	}

	public Integer getDataQueueSize() {
		return dataQueueSize;
	}

	public void setDataQueueSize(Integer dataQueueSize) {
		this.dataQueueSize = dataQueueSize;
	}

}
