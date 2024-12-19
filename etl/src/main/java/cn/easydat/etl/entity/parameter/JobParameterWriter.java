package cn.easydat.etl.entity.parameter;

import java.util.Arrays;

public class JobParameterWriter {

	private String writeMode;
	private String preSql;
	private String tableName;
	private String[] column;
	private String postSql;
	private Integer batchSize;

	private JobParameterJdbc jdbc;

	public String getWriteMode() {
		return writeMode;
	}

	public void setWriteMode(String writeMode) {
		this.writeMode = writeMode;
	}

	public String getPreSql() {
		return preSql;
	}

	public void setPreSql(String preSql) {
		this.preSql = preSql;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String[] getColumn() {
		return column;
	}

	public void setColumn(String[] column) {
		this.column = column;
	}

	public String getPostSql() {
		return postSql;
	}

	public void setPostSql(String postSql) {
		this.postSql = postSql;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public JobParameterJdbc getJdbc() {
		return jdbc;
	}

	public void setJdbc(JobParameterJdbc jdbc) {
		this.jdbc = jdbc;
	}

	@Override
	public String toString() {
		return "JobParameterWriter [writeMode=" + writeMode + ", preSql=" + preSql + ", tableName=" + tableName + ", column=" + Arrays.toString(column) + ", postSql=" + postSql + ", batchSize="
				+ batchSize + ", jdbc=" + jdbc + "]";
	}

}
