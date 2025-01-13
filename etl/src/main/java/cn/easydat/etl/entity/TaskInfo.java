package cn.easydat.etl.entity;

/**
 * job子任务
 */
public class TaskInfo {
	private int id;
	private String sql;
	private String deleteSql;

	public TaskInfo() {
		super();
	}

	public TaskInfo(int id, String sql, String deleteSql) {
		super();
		this.id = id;
		this.sql = sql;
		this.deleteSql = deleteSql;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getDeleteSql() {
		return deleteSql;
	}

	public void setDeleteSql(String deleteSql) {
		this.deleteSql = deleteSql;
	}

}
