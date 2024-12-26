package cn.easydat.etl.entity;

/**
 * job子任务
 */
public class TaskInfo {
	private int id;
	private String sql;

	public TaskInfo() {
		super();
	}

	public TaskInfo(int id, String sql) {
		super();
		this.id = id;
		this.sql = sql;
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

}
