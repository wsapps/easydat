package cn.easydat.etl.entity.parameter;

public class SplitPk {
	private String pkName;
	private String pkDataType;

	public SplitPk() {
		super();
	}

	public SplitPk(String pkName, String pkDataType) {
		super();
		this.pkName = pkName;
		this.pkDataType = pkDataType;
	}

	public String getPkName() {
		return pkName;
	}

	public void setPkName(String pkName) {
		this.pkName = pkName;
	}

	public String getPkDataType() {
		return pkDataType;
	}

	public void setPkDataType(String pkDataType) {
		this.pkDataType = pkDataType;
	}

	@Override
	public String toString() {
		return "SplitPk [pkName=" + pkName + ", pkDataType=" + pkDataType + "]";
	}
}