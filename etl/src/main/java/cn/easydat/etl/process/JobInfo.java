package cn.easydat.etl.process;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.MetaData;
import cn.easydat.etl.entity.TaskNode;
import cn.easydat.etl.entity.parameter.JobParameterWriter;
import cn.easydat.etl.util.DBUtil;

public class JobInfo {

	private static final Logger LOG = LoggerFactory.getLogger(JobInfo.class);

	// job参数
	private final JobParameter parameter;

	// 生产者任务执行完成
	private final Map<Integer, Boolean> subReaderFinish;
	private final BlockingQueue<Object[]> dataQueue;
	private final String writeSql;

	private volatile boolean allReaderFinish;
	private volatile List<MetaData> metaDatas;

	private AtomicLong monitorReaderRowNum;
	private AtomicLong monitorWriterRowNum;
	private long[] monitorProcessRowNumArr;

	private long monitorStartTime;

	private TaskNode taskNode;

	public JobInfo(JobParameter parameter) {
		this.parameter = parameter;

		this.allReaderFinish = false;
		this.subReaderFinish = new HashMap<>();
		this.dataQueue = new LinkedBlockingQueue<>(parameter.getSetting().getMaxNumOfChannel());

		this.writeSql = initWriteSql(parameter.getWriter());

		this.monitorWriterRowNum = new AtomicLong(0);
		this.monitorReaderRowNum = new AtomicLong(0);
		this.monitorStartTime = System.currentTimeMillis();
	}

	/**
	 * @return 返回Job参数
	 */
	public JobParameter getParameter() {
		return parameter;
	}

	/**
	 * @return 是否生产者执行完成
	 */
	public boolean isAllReaderFinish() {
		return allReaderFinish;
	}

	/**
	 * 设置生产者执行状态
	 */
	public void setAllReaderFinish(boolean allReaderFinish) {
		this.allReaderFinish = allReaderFinish;
	}

	public boolean isSubReaderFinish(int readerThreadNo) {
		return this.subReaderFinish.get(readerThreadNo);
	}

	public void setSubReaderFinish(int readerThreadNo, boolean allReaderFinish) {
		this.subReaderFinish.put(readerThreadNo, allReaderFinish);
	}

	/**
	 * 向队列添加数据信息，队列大小达到Channel数阻塞
	 */
	public void dataQueuePut(Object[] data) {
		int i = 0;
		boolean flag = false;
		while (!flag) {
			try {
				flag = dataQueue.offer(data, 500, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				LOG.error("dataQueuePut error", e);
				Thread.currentThread().interrupt();
			}
			i++;

			if (i % 100 == 0) {
				LOG.warn("dataQueuePut wait " + i);
			}
		}
	}

	/**
	 * @return 获取数据信息-阻塞
	 */
	public Object[] dataQueueTake() {
		Object[] data = null;
		try {
			data = dataQueue.poll(500, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			LOG.error("dataQueueTake error", e);
			Thread.currentThread().interrupt();
		}
		return data;
	}

	public boolean dataQueueIsEmpty() {
		return dataQueue.isEmpty();
	}

	public int dataQueueSize() {
		return dataQueue.size();
	}

	/**
	 * 生产者任务全部执行完成且任务队列为空返回True
	 */
	public boolean isTaskQueueFinish() {
		return allReaderFinish;
	}

	public String getWriteSql() {
		return writeSql;
	}

	public List<MetaData> getMetaDatas(ResultSet rs) {
		if (null == metaDatas) {
			synchronized (this) {
				if (null == metaDatas) {
					loadMetaData(rs);
				}
			}
		}
		return metaDatas;
	}

	public void monitorReaderRowNumAdd(long num) {
		this.monitorReaderRowNum.addAndGet(num);
	}

	public long monitorReaderRowNumGet() {
		return this.monitorReaderRowNum.get();
	}

	public void monitorWriterRowNumAdd(long num) {
		this.monitorWriterRowNum.addAndGet(num);
	}

	public long monitorWriterRowNumGet() {
		return this.monitorWriterRowNum.get();
	}

	public long getMonitorStartTime() {
		return monitorStartTime;
	}

	private void loadMetaData(ResultSet rs) {

		if (null != metaDatas) {
			return;
		}

		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();

			List<MetaData> metaDataList = new ArrayList<>(columnCount);

			for (int i = 1; i <= columnCount; i++) {
				MetaData md = new MetaData(metaData.getColumnLabel(i), metaData.getColumnName(i), metaData.getColumnTypeName(i), metaData.getColumnType(i));
				metaDataList.add(md);
			}

			this.metaDatas = metaDataList;
		} catch (SQLException e) {
			LOG.error("", e);
			throw new RuntimeException(e);
		}
	}

	private String initWriteSql(JobParameterWriter writer) {
		String fields = DBUtil.getFields(writer.getColumn());
		return String.format("INSERT INTO %s (%s) VALUES(%s)", writer.getTableName(), fields, createPlaceholders(writer.getColumn().length));
	}

	private String createPlaceholders(int count) {
		StringBuilder sb = new StringBuilder();
		sb.append("?");
		for (int i = 1; i < count; i++) {
			sb.append(",?");
		}
		return sb.toString();
	}

	public TaskNode getTaskNode() {
		return taskNode;
	}
	
	public long[] getMonitorProcessRowNumArr() {
		return monitorProcessRowNumArr;
	}

	public void setTaskNode(TaskNode taskNode) {
		this.taskNode = taskNode;
		this.monitorProcessRowNumArr = new long[taskNode.getReadSqlList().size()];
	}

}
