package cn.easydat.etl.process;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.MetaData;
import cn.easydat.etl.entity.parameter.JobParameterWriter;
import cn.easydat.etl.util.DBUtil;

public class JobInfo {
	
	private static final Logger LOG = LoggerFactory.getLogger(JobInfo.class);

	// job参数
	private JobParameter parameter;
	
	// 生产者任务执行完成
	private boolean allReaderFinish;
	private Map<Integer, Boolean> subReaderFinish;
	
	// db MetaData
	private List<MetaData> metaDatas;
	
	// 数据队列
	private Queue<List<Object>> dataQueue;
	
	private String writeSql;
	
	private AtomicLong monitorReaderRowNum;
	
	private AtomicLong monitorWriterRowNum;
	
	private long monitorStartTime;

	public JobInfo(JobParameter parameter) {
		super();

		this.parameter = parameter;

		this.allReaderFinish = false;
		this.subReaderFinish = new HashMap<Integer, Boolean>();
		this.dataQueue = new LinkedBlockingQueue<List<Object>>(parameter.getSetting().getMaxNumOfChannel());
		initDBConnection();
		
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
	
	public void setSubReaderFinish(int readerThreadNo ,boolean allReaderFinish) {
		this.subReaderFinish.put(readerThreadNo, allReaderFinish);
	}
	
	/**
	 * 向队列添加数据信息，队列大小达到Channel数阻塞
	 */
	public void dataQueueOffer(List<Object> data) {
		while (true) {
			boolean isOk = dataQueue.offer(data);
			if (isOk) {
				this.monitorReaderRowNum.addAndGet(1);
				break;
			} else {
				try {
					Thread.sleep(10);
//					System.out.println(readerThreadNo + ",reader block sleep 500");
				} catch (InterruptedException e) {
					LOG.error("", e);
				}
			}
		}
	}
	
	/**
	 * @return 获取数据信息-非阻塞
	 */
	public List<Object> dataQueuePoll() {
		return dataQueue.poll();
	}
	
	/**
	 * @return 获取数据信息-阻塞
	 */
	public List<Object> dataQueuePollBlock() {
		List<Object> data = null;
		while (true) {
			data = dataQueue.poll();

			if (null != data) {
				break;
			} else {
				if (this.allReaderFinish) {
					break;
				} else {
					try {
						Thread.sleep(200);
//						System.out.println(readerThreadNo + ",writer block sleep 500");
					} catch (InterruptedException e) {
						LOG.error("", e);
					}
				}
			}
		}
		return data;
	}
	
	public boolean dataQueueIsEmpty(){
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
			loadMetaDatas(rs);
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

	private synchronized void loadMetaDatas(ResultSet rs) {
		
		if (null != metaDatas) {
			return;
		}
		
		try {
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();

			List<MetaData> metaDataList = new ArrayList<MetaData>(columnCount);

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
	
	private void initDBConnection() {
		JobParameterWriter writer = parameter.getWriter();
		String fields = DBUtil.getFields(writer.getColumn());
		this.writeSql = String.format("INSERT INTO %s (%s) VALUES(%s)", writer.getTableName(), fields, insertValues(writer.getColumn().length));
	}
	
	private String insertValues(int num) {
		String val = "?";
		for (int i = 1; i < num; i++) {
			val += ",?";
		}
		return val;
	}

}
