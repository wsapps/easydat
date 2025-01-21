package cn.easydat.etl.entity.parameter;

public class JobParameterSetting {
	private Integer channel;
	private Integer maxNumOfChannel;
	private Integer splitMax;

	public Integer getChannel() {
		return channel;
	}

	public void setChannel(Integer channel) {
		this.channel = channel;
	}

	public Integer getMaxNumOfChannel() {
		return maxNumOfChannel;
	}

	public void setMaxNumOfChannel(Integer maxNumOfChannel) {
		this.maxNumOfChannel = maxNumOfChannel;
	}

	public Integer getSplitMax() {
		return splitMax;
	}

	public void setSplitMax(Integer splitMax) {
		this.splitMax = splitMax;
	}

	@Override
	public String toString() {
		return "JobParameterSetting [channel=" + channel + ", maxNumOfChannel=" + maxNumOfChannel + ", splitMax=" + splitMax + "]";
	}

}
