package cn.easydat.etl.job.service;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.noear.solon.annotation.Component;
import org.noear.solon.annotation.Inject;
import org.noear.solon.cloud.CloudClient;
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
import cn.easydat.etl.entity.parameter.JobParameterSetting;
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

	public Long createJobProcess(Integer jobId) {
		long processNo = CloudClient.id().generate();

		try {
			BigInteger jobLogId = sqlUtils.sql("INSERT INTO etl_job_log(job_id,run_status,run_time_start) VALUES (?,?,?)", jobId, 1, new Date()).updateReturnKey();
			sqlUtils.sql("INSERT INTO etl_job_task_log(job_log_id,task_id,job_id,run_status) SELECT ?,id,?,0 FROM etl_task where id in (SELECT task_id from etl_job_task where job_id=?)",
					jobLogId, jobId, jobId).update();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return processNo;
	}

	public Integer executeTask() {
		Map<String, Object> taskInfo = getExecutableTask();
		Integer taskId = null;
		if (null != taskInfo) {
			taskId = (Integer) taskInfo.get("task_id");
			JobParameter jobParameter = null;
			
			/**
				 SELECT
					CONCAT('`',a.r_table_schema,'`.`',a.r_tablename,'`')  'reader.tableName',
					a.reader_column 'reader.column',
					a.split_pk 'reader.splitPk.pkName',
					a.pkType 'reader.splitPk.pkDataType',
					a.`where` 'reader.where',
					a.query_sql 'reader.querySql',
					CONCAT('`',a.w_table_schema,'`.`',a.w_tablename,'`')  'writer.tableName',
					a.writer_column 'writer.column',
					a.pre_sql 'writer.preSql',
					r.driver 'reader.jdbc.driver',
					r.jdbc 'reader.jdbc.jdbcUrl',
					r.username 'reader.jdbc.username',
					r.pwd 'reader.jdbc.password',
					w.driver 'writer.jdbc.driver',
					w.jdbc 'writer.jdbc.jdbcUrl',
					w.username 'writer.jdbc.username',
					w.pwd 'writer.jdbc.password'
				FROM
					etl_task a
					LEFT JOIN etl_datasource r ON a.r_ds_id = r.id
					LEFT JOIN etl_datasource w ON a.w_ds_id = w.id 
				WHERE
					a.id =?
			 */
			String queryTaskSql = "SELECT\r\n"
					+ "	CONCAT('`',a.r_table_schema,'`.`',a.r_tablename,'`')  'reader.tableName',\r\n"
					+ "	a.reader_column 'reader.column',\r\n"
					+ "	a.split_pk 'reader.splitPk.pkName',\r\n"
					+ "	a.pkType 'reader.splitPk.pkDataType',\r\n"
					+ "	a.`where` 'reader.where',\r\n"
					+ "	a.query_sql 'reader.querySql',\r\n"
					+ "	CONCAT('`',a.w_table_schema,'`.`',a.w_tablename,'`')  'writer.tableName',\r\n"
					+ "	a.writer_column 'writer.column',\r\n"
					+ "	a.pre_sql 'writer.preSql',\r\n"
					+ "	r.driver 'reader.jdbc.driver',\r\n"
					+ "	r.jdbc 'reader.jdbc.jdbcUrl',\r\n"
					+ "	r.username 'reader.jdbc.username',\r\n"
					+ "	r.pwd 'reader.jdbc.password',\r\n"
					+ "	w.driver 'writer.jdbc.driver',\r\n"
					+ "	w.jdbc 'writer.jdbc.jdbcUrl',\r\n"
					+ "	w.username 'writer.jdbc.username',\r\n"
					+ "	w.pwd 'writer.jdbc.password'\r\n"
					+ "FROM\r\n"
					+ "	etl_task a\r\n"
					+ "	LEFT JOIN etl_datasource r ON a.r_ds_id = r.id\r\n"
					+ "	LEFT JOIN etl_datasource w ON a.w_ds_id = w.id \r\n"
					+ "WHERE\r\n"
					+ "	a.id =?";
			
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
			
			JobParameterSetting jobParameterSetting = new JobParameterSetting();
			jobParameterSetting.setChannel(channel);
			jobParameterSetting.setMaxNumOfChannel(maxNumOfChannel);
			jobParameter.setSetting(jobParameterSetting);
			jobParameter.getWriter().setBatchSize(batchSize);
			jobParameter.setJobNo(taskInfo.get("id").toString());
			jobParameter.setCustomMonitor(new SimpleCustomMonitor());
			
			int status = 2;
			FinishInfo finishInfo = null;
			try {
				EtlTaskMain etlTaskMain = new EtlTaskMain();
				finishInfo = etlTaskMain.startup(jobParameter);
			} catch (Throwable e) {
				LOGGER.error("taskId:" + taskId, e);
				status = -1;
			}

			try {
				if (status == -1) {
					sqlUtils.sql("update etl_job_task_log set run_status=?,run_time_end=? where id=?", status, new Date(), taskInfo.get("id")).update();
				} else if (status == 2) {
					sqlUtils.sql("update etl_job_task_log set run_status=2,run_time_start=?,run_time_end=?,runtime=?,reader_row_num=?,writer_row_num=? where id=?", new Date(finishInfo.getStartTime()),
							new Date(finishInfo.getEndTime()), finishInfo.getRuntime(), finishInfo.getReaderRowNum(), finishInfo.getWriterRowNum(), taskInfo.get("id")).update();
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		
		return taskId;
	}

	private Map<String, Object> getExecutableTask() {
		Map<String, Object> taskInfo = new HashMap<String, Object>();
		String querySql = "select * from etl_job_task_log where run_status = 0 limit 1 for update";
		String updateSql = "update etl_job_task_log set run_status=1,run_time_start=?,run_server=? where id=?";

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

}
