package cn.easydat.etl.entity.parameter;

public class JobParameterSetting {
	private Integer channel;
	private Integer maxNumOfChannel;

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

	@Override
	public String toString() {
		return "JobParameterSetting [channel=" + channel + ", maxNumOfChannel=" + maxNumOfChannel + "]";
	}

}
