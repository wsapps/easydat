package cn.easydat.etl.job.service;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.noear.solon.annotation.Component;
import org.noear.solon.annotation.Inject;
import org.noear.solon.data.annotation.TranAnno;
import org.noear.solon.data.sql.SqlUtils;
import org.noear.solon.data.sql.bound.RowConverter;
import org.noear.solon.data.tran.TranIsolation;
import org.noear.solon.data.tran.TranUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.EtlTaskMain;
import cn.easydat.etl.entity.FinishInfo;
import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.TaskNode;
import cn.easydat.etl.entity.parameter.JobParameterSetting;
import cn.easydat.etl.entity.parameter.JobParameterWriter;
import cn.easydat.etl.process.pre.Preprocessing;
import cn.easydat.etl.process.producer.SplitTask;
import cn.easydat.etl.util.DBUtil;
import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONObject;

@Component
public class JobService {

	private static final Logger LOGGER = LoggerFactory.getLogger(JobService.class);

	@Inject("etlDataSource")
	private SqlUtils sqlUtils;

	@Inject("${easydate.etl.job.serverName}")
	public String serverName;

	@Inject("${easydate.etl.job.channel}")
	public Integer channel;

	@Inject("${easydate.etl.job.maxNumOfChannel}")
	public Integer maxNumOfChannel;

	@Inject("${easydate.etl.job.batchSize}")
	public Integer batchSize;

	public BigInteger runJob(Integer jobId) {
		BigInteger processNo = createJobProcess(jobId);
		// createJobTask(processNo);

		return processNo;
	}

	public Integer executeTask() {
		Map<String, Object> taskInfo = getExecutableTask();
		Integer taskId = null;
		if (null != taskInfo) {
			taskId = (Integer) taskInfo.get("task_id");
			JobParameter jobParameter = taskInfoToJobParameter(taskInfo);

			int status = 2;
			FinishInfo finishInfo = null;
			try {
				EtlTaskMain etlTaskMain = new EtlTaskMain();
				finishInfo = etlTaskMain.startup(jobParameter);
				status = finishInfo.getSuccess() ? 2 : -1;
			} catch (Throwable e) {
				LOGGER.error("taskId:" + taskId, e);
				status = -1;
			}

			try {
				if (status == -1) {
					sqlUtils.sql("update etl_job_task_process set run_status=?,run_time_end=? where id=?", status, new Date(), taskInfo.get("id")).update();
				} else if (status == 2) {
					sqlUtils.sql("update etl_job_task_process set run_status=2,run_time_start=?,run_time_end=?,runtime=?,reader_row_num=?,writer_row_num=? where id=?",
							new Date(finishInfo.getStartTime()), new Date(finishInfo.getEndTime()), finishInfo.getRuntime(), finishInfo.getReaderRowNum(), finishInfo.getWriterRowNum(),
							taskInfo.get("id")).update();
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		return taskId;
	}

	public void createAndRunJobTask(BigInteger processNo) {

		while (true) {
			Integer taskId = createJobTask(processNo);
			if (null == taskId) {
				break;
			}
		}

		String updateSql = "update etl_job_process set run_status=2,run_time_end=? where id=?";
		try {
			sqlUtils.sql(updateSql, new Date(), processNo).update();
		} catch (SQLException e) {
			LOGGER.error("processNo:" + processNo, e);
		}

	}

	public Integer createJobTask(BigInteger processNo) {
		Integer taskId = null;
		Date date = new Date();
		Map<String, Object> taskInfo = getExecutableTaskByProcessNo(processNo, date);

		if (null != taskInfo) {
			Integer jobId = (Integer) taskInfo.get("job_id");
			taskId = (Integer) taskInfo.get("task_id");
			Long id = toLong(taskInfo.get("id"));
			JobParameter jobParameter = taskInfoToJobParameter(taskInfo);
			createJobTaskNode(processNo, jobId, taskId, jobParameter);

			Preprocessing preprocessing = new Preprocessing();
			preprocessing.startup(jobParameter);

			executeJobTask(jobParameter, processNo, taskId, date, id);
		}
		return taskId;
	}

	public void recoveringIncompleteExecution() {
		String updateSql = "UPDATE etl_job_task_node_process SET run_status=-1 WHERE run_status=1";
		try {
			LOGGER.info("recoveringIncompleteExecution");
			sqlUtils.sql(updateSql).update();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

		while (true) {
			Long id = getIncompleteExecution();

			if (null == id) {
				break;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Long getIncompleteExecution() {
		String findTaskNodeProcessSql = "SELECT * FROM etl_job_task_node_process WHERE run_status IN (-1,0) limit 1";
		String findTaskProcessSql = "select * from etl_job_task_process where job_process_id=? and task_id=? and run_status=1";
		Long taskProcessId = null;
		try {
			Map<String, Object> nodeProcess = sqlUtils.sql(findTaskNodeProcessSql).queryRow(Map.class);
			
			if (null != nodeProcess) {

				Long jobProcessId = toLong(nodeProcess.get("job_process_id"));
				Long taskId = toLong(nodeProcess.get("task_id"));
				
				Map<String, Object> taskProcess = sqlUtils.sql(findTaskProcessSql, jobProcessId, taskId).queryRow(Map.class);
				taskProcessId = toLong(taskProcess.get("id"));
				JobParameter jobParameter = taskInfoToJobParameter(taskProcess);
				
				executeJobTask(jobParameter, BigInteger.valueOf(jobProcessId), taskId.intValue(), new Date(), taskProcessId);
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		
		return taskProcessId;
	}

	private BigInteger createJobProcess(Integer jobId) {
		BigInteger jobLogId = null;
		try {
			jobLogId = sqlUtils.sql("INSERT INTO etl_job_process(job_id,run_status,run_time_start) VALUES (?,?,?)", jobId, 1, new Date()).updateReturnKey();
			sqlUtils.sql("INSERT INTO etl_job_task_process(job_process_id,task_id,job_id,run_status) SELECT ?,id,?,0 FROM etl_task where id in (SELECT task_id from etl_job_task where job_id=?)",
					jobLogId, jobId, jobId).update();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return jobLogId;
	}

	private void createJobTaskNode(BigInteger processNo, Integer jobId, Integer taskId, JobParameter jobParameter) {
		SplitTask splitTask = new SplitTask();
		TaskNode taskNode = splitTask.split(jobParameter);
		String sql = "INSERT INTO etl.etl_job_task_node_process( job_process_id, job_id, task_id, run_status, read_sql, write_sql,delete_sql) VALUES (?, ?, ?, ?, ?, ?,?)";

		String writeSql = initWriteSql(jobParameter.getWriter());

		if (null != taskNode && null != taskNode.getReadSqlList() && !taskNode.getReadSqlList().isEmpty()) {
			List<Object[]> argsList = new ArrayList<Object[]>();
			for (int i = 0; i < taskNode.getReadSqlList().size(); i++) {
				String readSql = taskNode.getReadSqlList().get(i);
				String deleteSql = taskNode.getDeleteSqlList().get(i);

				Object[] obj = { processNo, jobId, taskId, 0, readSql, writeSql, deleteSql };
				argsList.add(obj);
			}

			try {
				sqlUtils.sql(sql).updateBatch(argsList);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private JobParameter taskInfoToJobParameter(Map<String, Object> taskInfo) {
		JobParameter jobParameter = null;

		if (null != taskInfo) {
			Integer taskId = (Integer) taskInfo.get("task_id");
			/**
			 * SELECT CONCAT('`',a.r_table_schema,'`.`',a.r_tablename,'`')
			 * 'reader.tableName', a.reader_column 'reader.column', a.split_pk
			 * 'reader.splitPk.pkName', a.pkType 'reader.splitPk.pkDataType', a.`where`
			 * 'reader.where', a.query_sql 'reader.querySql',
			 * CONCAT('`',a.w_table_schema,'`.`',a.w_tablename,'`') 'writer.tableName',
			 * a.writer_column 'writer.column', a.pre_sql 'writer.preSql', r.driver
			 * 'reader.jdbc.driver', r.jdbc 'reader.jdbc.jdbcUrl', r.username
			 * 'reader.jdbc.username', r.pwd 'reader.jdbc.password', w.driver
			 * 'writer.jdbc.driver', w.jdbc 'writer.jdbc.jdbcUrl', w.username
			 * 'writer.jdbc.username', w.pwd 'writer.jdbc.password', a.channel
			 * 'setting.channel', a.max_num_of_channel 'setting.maxNumOfChannel',
			 * a.batch_size 'writer.batchSize', a.split_max 'setting.splitMax' FROM etl_task
			 * a LEFT JOIN etl_datasource r ON a.r_ds_id = r.id LEFT JOIN etl_datasource w
			 * ON a.w_ds_id = w.id WHERE a.id =?
			 */
			String queryTaskSql = "SELECT\r\n" + "	CONCAT('`',a.r_table_schema,'`.`',a.r_tablename,'`')  'reader.tableName',\r\n" + "	a.reader_column 'reader.column',\r\n"
					+ "	a.split_pk 'reader.splitPk.pkName',\r\n" + "	a.pkType 'reader.splitPk.pkDataType',\r\n" + "	a.`where` 'reader.where',\r\n" + "	a.query_sql 'reader.querySql',\r\n"
					+ "	CONCAT('`',a.w_table_schema,'`.`',a.w_tablename,'`')  'writer.tableName',\r\n" + "	a.writer_column 'writer.column',\r\n" + "	a.pre_sql 'writer.preSql',\r\n"
					+ "	r.driver 'reader.jdbc.driver',\r\n" + "	r.jdbc 'reader.jdbc.jdbcUrl',\r\n" + "	r.username 'reader.jdbc.username',\r\n" + "	r.pwd 'reader.jdbc.password',\r\n"
					+ "	w.driver 'writer.jdbc.driver',\r\n" + "	w.jdbc 'writer.jdbc.jdbcUrl',\r\n" + "	w.username 'writer.jdbc.username',\r\n" + "	w.pwd 'writer.jdbc.password',\r\n"
					+ " a.channel 'setting.channel',a.max_num_of_channel 'setting.maxNumOfChannel',a.batch_size 'writer.batchSize', a.split_max 'setting.splitMax'" + "FROM\r\n" + "	etl_task a\r\n"
					+ "	LEFT JOIN etl_datasource r ON a.r_ds_id = r.id\r\n" + "	LEFT JOIN etl_datasource w ON a.w_ds_id = w.id \r\n" + "WHERE\r\n" + "	a.id =?";

			try {
				jobParameter = sqlUtils.sql(queryTaskSql, taskId).queryRow(new RowConverter<JobParameter>() {
					@Override
					public JobParameter convert(ResultSet rs) throws SQLException {
						ResultSetMetaData metaData = rs.getMetaData();
						JSONObject data = new JSONObject();

						for (int i = 1; i <= metaData.getColumnCount(); i++) {
							String name = metaData.getColumnLabel(i);
							Object value = rs.getObject(i);
							data.putByPath(name, value);
						}

						return BeanUtil.toBean(data, JobParameter.class);
					}
				});
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}

			if (null == jobParameter.getSetting()) {
				jobParameter.setSetting(new JobParameterSetting());
			}

			if (null == jobParameter.getSetting().getChannel()) {
				jobParameter.getSetting().setChannel(channel);
			}
			if (null == jobParameter.getSetting().getMaxNumOfChannel()) {
				jobParameter.getSetting().setMaxNumOfChannel(maxNumOfChannel);
			}
			if (null == jobParameter.getWriter().getBatchSize()) {
				jobParameter.getWriter().setBatchSize(batchSize);
			}

			jobParameter.setJobNo(taskInfo.get("id").toString());
			jobParameter.setCustomMonitor(new SimpleCustomMonitor());
		}
		return jobParameter;
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

	private Map<String, Object> getExecutableTask() {
		Map<String, Object> taskInfo = new HashMap<String, Object>();
		String querySql = "select * from etl_job_task_process where run_status = 0 limit 1 for update";
		String updateSql = "update etl_job_task_process set run_status=1,run_time_start=?,run_server=? where id=?";

		try {
			TranUtils.execute(new TranAnno().isolation(TranIsolation.serializable), () -> {
				@SuppressWarnings("unchecked")
				Map<String, Object> map = sqlUtils.sql(querySql).queryRow(Map.class);
				if (null != map) {
					taskInfo.putAll(map);
					Object id = taskInfo.get("id");
					sqlUtils.sql(updateSql, new Date(), serverName, id).update();
				}

			});
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

		if (taskInfo.containsKey("id")) {
			return taskInfo;
		} else {
			return null;
		}
	}

	private Map<String, Object> getExecutableTaskByProcessNo(BigInteger processNo, Date date) {
		Map<String, Object> taskInfo = new HashMap<String, Object>();
		String querySql = "select * from etl_job_task_process where job_process_id=? and run_status = 0 limit 1 for update";
		String updateSql = "update etl_job_task_process set run_status=1,run_time_start=?,run_server=? where id=?";

		try {
			TranUtils.execute(new TranAnno().isolation(TranIsolation.serializable), () -> {
				@SuppressWarnings("unchecked")
				Map<String, Object> map = sqlUtils.sql(querySql, processNo).queryRow(Map.class);
				if (null != map) {
					taskInfo.putAll(map);
					Object id = taskInfo.get("id");
					taskInfo.put("run_time_start", date);
					sqlUtils.sql(updateSql, date, serverName, id).update();
				}

			});
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

		if (taskInfo.containsKey("id")) {
			return taskInfo;
		} else {
			return null;
		}
	}

	private void executeJobTask(JobParameter jobParameter, BigInteger processNo, Integer taskId, Date date, Long id) {
		int channel = jobParameter.getSetting().getChannel();

		ExecutorService executorService = new ThreadPoolExecutor(channel, channel, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(channel));
		
		SimpleConnectionPool readPool = null;
		SimpleConnectionPool writePool = null;
		
		try {
			readPool = new SimpleConnectionPool(jobParameter.getReader().getJdbc(), channel, channel + 1);
			writePool = new SimpleConnectionPool(jobParameter.getWriter().getJdbc(), channel, channel + 1);
		} catch (SQLException e) {
			LOGGER.error("SimpleConnectionPool error", e);
		}
		

		for (int i = 0; i < channel; i++) {
			final SimpleConnectionPool rp = readPool;
			final SimpleConnectionPool wp = writePool;
			executorService.submit(() -> {
				executeJobTaskNode(jobParameter, processNo, taskId, rp, wp);
			});
		}

		executorService.shutdown();

		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOGGER.error("processNo:" + processNo + ", taskId:" + taskId, e);
			}
		}
		
		try {
			LOGGER.info("Pool shutdown start");
			readPool.shutdown();
			writePool.shutdown();
			LOGGER.info("Pool shutdown end");
		} catch (SQLException e) {
			LOGGER.error("processNo:" + processNo + ", taskId:" + taskId, e);
		}
		
		Date runTimeEnd = new Date();
		long runtime = (runTimeEnd.getTime() - date.getTime()) / 1000;
		String updateSql = "update etl_job_task_process set run_status=2,run_time_end=?,runtime=? where id=?";
		try {
			sqlUtils.sql(updateSql, runTimeEnd, runtime, id).update();
		} catch (SQLException e) {
			LOGGER.error("processNo:" + processNo + ", taskId:" + taskId, e);
		}

	}

	private void executeJobTaskNode(JobParameter jobParameter, BigInteger processNo, Integer taskId, SimpleConnectionPool readPool, SimpleConnectionPool writePool) {
		String updateNodeSuccessSql = "update etl_job_task_node_process set run_status=2,run_time_end=?,runtime=?,writer_row_num=? where id=?";
		String updateNodeFailureSql = "update etl_job_task_node_process set run_status=-1,retry_times=? where id=?";
		while (true) {
			Map<String, Object> nodeInfo = findOneNode(processNo, taskId);
			if (!nodeInfo.isEmpty()) {
				Long id = toLong(nodeInfo.get("id"));
				String readSql = (String) nodeInfo.get("read_sql");
				String writeSql = (String) nodeInfo.get("write_sql");
				String deleteSql = (String) nodeInfo.get("delete_sql");
				Date runTimeStart = (Date) nodeInfo.get("run_time_start");
				Integer retryTimes = (Integer) nodeInfo.get("retry_times");
				Integer runStatus = (Integer) nodeInfo.get("run_status");
				Consumer consumer = new Consumer(readSql, writeSql, deleteSql, jobParameter, readPool, writePool);
				boolean error = false;
				try {
					error = consumer.run(id, runStatus);
				} catch (Throwable e) {
					LOGGER.error("run,sql:" + readSql, e);
					error = true;
				}
				Date runTimeEnd = new Date();
				long runtime = (runTimeEnd.getTime() - runTimeStart.getTime()) / 1000;

				if (!error) {
					try {
						sqlUtils.sql(updateNodeSuccessSql, runTimeEnd, runtime, consumer.getWriteNum(), id).update();
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				} else {
					try {
						retryTimes = (null == retryTimes ? 0 : retryTimes);
						retryTimes++;
						sqlUtils.sql(updateNodeFailureSql, retryTimes, id).update();
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
				}

			} else {
				break;
			}
		}
	}

	private Map<String, Object> findOneNode(BigInteger processNo, Integer taskId) {
		String queryNodeSql = "SELECT * FROM etl_job_task_node_process WHERE job_process_id=? AND task_id=? AND run_status IN (-1,0) limit 1 FOR UPDATE";
		String updateNodeSql = "UPDATE etl_job_task_node_process SET run_status=1,run_time_start=?,run_server=? WHERE id=?";
		Map<String, Object> nodeInfo = new HashMap<String, Object>();

		try {
			TranUtils.execute(new TranAnno().isolation(TranIsolation.serializable), () -> {
				@SuppressWarnings("unchecked")
				Map<String, Object> map = sqlUtils.sql(queryNodeSql, processNo, taskId).queryRow(Map.class);
				if (null != map) {
					nodeInfo.putAll(map);
					Object id = nodeInfo.get("id");
					Date date = new Date();
					nodeInfo.put("run_time_start", date);
					sqlUtils.sql(updateNodeSql, date, serverName, id).update();
				}

			});
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
		return nodeInfo;
	}

	private Long toLong(Object obj) {
		Long l = null;
		if (null != obj) {
			if (obj instanceof BigInteger) {
				l = ((BigInteger) obj).longValue();
			} else if (obj instanceof Long) {
				l = (Long) obj;
			} else if (obj instanceof Integer) {
				l = ((Integer) obj).longValue();
			}
		}
		return l;
	}

}
