package cn.easydat.etl.entity;

public class MetaData {
	private String columnLabel;
	private String columnName;
	private String columnTypeName;
	private int columnType;

	public MetaData() {
		super();
	}

	public MetaData(String columnLabel, String columnName, String columnTypeName, int columnType) {
		super();
		this.columnLabel = columnLabel;
		this.columnName = columnName;
		this.columnTypeName = columnTypeName;
		this.columnType = columnType;
	}

	public String getColumnLabel() {
		return columnLabel;
	}

	public void setColumnLabel(String columnLabel) {
		this.columnLabel = columnLabel;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnTypeName() {
		return columnTypeName;
	}

	public void setColumnTypeName(String columnTypeName) {
		this.columnTypeName = columnTypeName;
	}

	public int getColumnType() {
		return columnType;
	}

	public void setColumnType(int columnType) {
		this.columnType = columnType;
	}

	@Override
	public String toString() {
		return "MetaData [columnLabel=" + columnLabel + ", columnName=" + columnName + ", columnTypeName=" + columnTypeName + ", columnType=" + columnType + "]";
	}

}
