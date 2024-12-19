package cn.easydat.etl.entity.parameter;

import java.util.Arrays;

public class JobParameterReader {

	private SplitPk splitPk;
	private String querySql;
	private String tableName;
	private String[] column;
	private String where;

	private JobParameterJdbc jdbc;

	public SplitPk getSplitPk() {
		return splitPk;
	}

	public void setSplitPk(SplitPk splitPk) {
		this.splitPk = splitPk;
	}

	public String getQuerySql() {
		return querySql;
	}

	public void setQuerySql(String querySql) {
		this.querySql = querySql;
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

	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public JobParameterJdbc getJdbc() {
		return jdbc;
	}

	public void setJdbc(JobParameterJdbc jdbc) {
		this.jdbc = jdbc;
	}

	@Override
	public String toString() {
		return "JobParameterReader [splitPk=" + splitPk + ", querySql=" + querySql + ", tableName=" + tableName
				+ ", column=" + Arrays.toString(column) + ", where=" + where + ", jdbc=" + jdbc + "]";
	}

}


