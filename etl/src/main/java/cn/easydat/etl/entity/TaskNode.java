package cn.easydat.etl.entity;

import java.util.List;

public class TaskNode {
	private List<String> readSqlList;
	private List<String> deleteSqlList;

	public TaskNode() {
		super();
	}

	public TaskNode(List<String> readSqlList, List<String> deleteSqlList) {
		super();
		this.readSqlList = readSqlList;
		this.deleteSqlList = deleteSqlList;
	}

	public List<String> getReadSqlList() {
		return readSqlList;
	}

	public void setReadSqlList(List<String> readSqlList) {
		this.readSqlList = readSqlList;
	}

	public List<String> getDeleteSqlList() {
		return deleteSqlList;
	}

	public void setDeleteSqlList(List<String> deleteSqlList) {
		this.deleteSqlList = deleteSqlList;
	}

}
