package cn.easydat.etl.process.producer;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.entity.TaskNode;
import cn.easydat.etl.entity.parameter.JobParameterReader;
import cn.easydat.etl.entity.parameter.JobParameterSetting;
import cn.easydat.etl.util.DBUtil;

public class SplitTask {

	private static final Logger LOG = LoggerFactory.getLogger(SplitTask.class);

	public TaskNode split(JobParameter parameter) {
		TaskNode taskNode = new TaskNode();
		List<String> sqls = null;

		if (null != parameter.getReader().getQuerySql()) {
			sqls = new ArrayList<String>(1);
			sqls.add(parameter.getReader().getQuerySql());
			taskNode.setReadSqlList(sqls);
			
			List<String> deleteSqls = new ArrayList<String>(1);
			deleteSqls.add("DELETE FROM " + parameter.getReader().getTableName());
			taskNode.setDeleteSqlList(deleteSqls);
		} else {
			if (null != parameter.getReader().getSplitPk() && null != parameter.getReader().getSplitPk().getPkName()) {
				if ("int".equals(parameter.getReader().getSplitPk().getPkDataType())) {
					List<String> wherePKSplit = intSplit(parameter);
					sqls = splitTask(parameter.getReader(), wherePKSplit);
					
					taskNode.setReadSqlList(sqls);
					taskNode.setDeleteSqlList(splitDeleteTask(parameter.getReader(), wherePKSplit));
				} else if ("varchar".equals(parameter.getReader().getSplitPk().getPkDataType())) {
					varcharSplit(parameter);
				} else {
					throw new RuntimeException("Error Type, pkDataType: " + parameter.getReader().getSplitPk().getPkDataType());
				}
			} else {
				String fields = DBUtil.getFields(parameter.getReader().getColumn());
				String sql = String.format("SELECT %s FROM %s", fields, parameter.getReader().getTableName());
				String delSql = String.format("DELETE FROM %s", parameter.getReader().getTableName());

				if (null != parameter.getReader().getWhere()) {
					sql += " WHERE " + parameter.getReader().getWhere();
					sqls = new ArrayList<String>(1);
					sqls.add(sql);
				}
				
				List<String> deleteSqls = new ArrayList<String>(1);
				deleteSqls.add(delSql);
				
				taskNode.setReadSqlList(sqls);
				taskNode.setDeleteSqlList(deleteSqls);
			}
		}
		return taskNode;
	}

	private List<String> intSplit(JobParameter parameter) {
		List<String> sqls = null;
		String pk = parameter.getReader().getSplitPk().getPkName();
		String table = parameter.getReader().getTableName();
		// String minMaxSql = String.format("SELECT min(%s) min, max(%s) max FROM %s",
		// pk, pk, table);

		String where = parameter.getReader().getWhere();
		String whereSql = "";
		if (null != where) {
			// TODO
//			whereSql = String.format(" WHERE %s ", where);
		}

		String minMaxSql = String.format("SELECT a.%s min,b.%s max from (SELECT %s FROM %s %s ORDER BY %s ASC LIMIT 1) a,(SELECT %s FROM %s %s ORDER BY %s DESC LIMIT 1) b", pk, pk, pk, table,
				whereSql, pk, pk, table, whereSql, pk);

		LOG.info("minMaxSql:" + minMaxSql);

		try (Connection conn = DBUtil.getConnection(parameter.getReader().getJdbc());
				Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = DBUtil.query(stmt, minMaxSql);) {

			if (rs.next()) {
				String minStr = rs.getString("min");
				String maxStr = rs.getString("max");
				BigInteger min = new BigInteger(minStr);
				BigInteger max = new BigInteger(maxStr);

				LOG.info("split min: {}, max: {}.", min, max);

				BigInteger[] pkArr = doBigIntegerSplit(min, max, parameter.getSetting());
				List<String> wherePKSplit = wherePKSplit(pkArr, pk);
//				sqls = splitTask(parameter.getReader(), wherePKSplit);
				sqls = wherePKSplit;
			}

			DBUtil.closeDBResources(rs, stmt, conn);
		} catch (SQLException e) {
			LOG.error("intSplit error, parameter:" + parameter, e);
		}
		return sqls;
	}

	private BigInteger[] doBigIntegerSplit(BigInteger min, BigInteger max, JobParameterSetting setting) {
		BigInteger[] result = null;
		// 左开右闭, 所以最小值减一
		min = min.subtract(BigInteger.ONE);
		if (min.compareTo(max) == 0) {
			result = new BigInteger[] { min, max };
		} else {

			int expectSliceNumber = setting.getChannel();
			BigInteger endAndStartGap = max.subtract(min).add(BigInteger.ONE) ;
			BigInteger step = endAndStartGap.divide(BigInteger.valueOf(expectSliceNumber));

			if (endAndStartGap.mod(BigInteger.valueOf(expectSliceNumber)).compareTo(BigInteger.ZERO) > 0) {
				step = step.add(BigInteger.ONE);
			}

			
			BigInteger maxStep = BigInteger.valueOf((null != setting.getSplitMax() ? setting.getSplitMax() : 50000));

			if (step.compareTo(maxStep) > 0) {
				step = maxStep;
			}

			List<BigInteger> list = new ArrayList<BigInteger>(expectSliceNumber + 1);
			list.add(min);
			BigInteger value = min;

			while (true) {
				value = value.add(step);

				if (value.compareTo(max) > 0) {
					value = max;
					list.add(value);
					break;
				} else {
					list.add(value);
				}
			}
			
			result = list.toArray(new BigInteger[list.size()]);
		}
		return result;
	}

	private List<String> wherePKSplit(BigInteger[] pkArr, String pk) {
		List<String> wheres = new ArrayList<>();
		for (int i = 0; i < pkArr.length - 1; i++) {
			String where = String.format(" %s > %s AND %s <= %s", pk, pkArr[i], pk, pkArr[i + 1]);
			wheres.add(where);
		}
		return wheres;
	}

	private List<String> splitTask(JobParameterReader reader, List<String> wherePKSplit) {
		List<String> sqls = new ArrayList<String>();
		String fields = DBUtil.getFields(reader.getColumn());

		String sql = String.format("SELECT %s FROM %s", fields, reader.getTableName());

		if (null != wherePKSplit && !wherePKSplit.isEmpty()) {
			for (String pkSplit : wherePKSplit) {
				String sqlSplit = sql + " WHERE " + pkSplit;

				if (null != reader.getWhere()) {
					sqlSplit += " AND " + reader.getWhere();
				}

				sqls.add(sqlSplit);
			}
		}

		return sqls;
	}
	
	private List<String> splitDeleteTask(JobParameterReader reader, List<String> wherePKSplit) {
		List<String> sqls = new ArrayList<String>();

		String sql = String.format("DELETE FROM %s", reader.getTableName());

		if (null != wherePKSplit) {
			for (String pkSplit : wherePKSplit) {
				String sqlSplit = sql + " WHERE " + pkSplit;
				sqls.add(sqlSplit);
			}
		}

		return sqls;
	}

	private void varcharSplit(JobParameter parameter) {
		// TODO
	}
}
