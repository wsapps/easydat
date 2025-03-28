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
import cn.easydat.etl.util.DBUtil;

public class SplitTaskEx {

	private static final Logger LOG = LoggerFactory.getLogger(SplitTaskEx.class);

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

		String where = parameter.getReader().getWhere();
//		String whereSql = "";
		if (null != where) {
			// TODO
//			whereSql = String.format(" WHERE %s ", where);
		}

		Integer splitMax = parameter.getSetting().getSplitMax();

		String sql = String.format("SELECT min( t.%s ) pk_min, max( t.%s ) pk_max FROM ( SELECT %s FROM %s ORDER BY %s LIMIT %s ) t", pk, pk, pk, table, pk, splitMax);

//		String minMaxSql = String.format("SELECT a.%s min,b.%s max from (SELECT %s FROM %s %s ORDER BY %s ASC LIMIT 1) a,(SELECT %s FROM %s %s ORDER BY %s DESC LIMIT 1) b", pk, pk, pk, table,
//				whereSql, pk, pk, table, whereSql, pk);
//
//		LOG.info("minMaxSql:" + minMaxSql);

		List<BigInteger[]> pkArr = new ArrayList<BigInteger[]>();

		try {
			Connection conn = DBUtil.getConnection(parameter.getReader().getJdbc());
			//Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			BigInteger[] minmax = pkIntMinMax(parameter, conn, sql);
			if (null != minmax) {
				pkArr.add(minmax);
				minmaxNext(parameter, conn, minmax[1], pkArr);
				List<String> wherePKSplit = wherePKSplit(pkArr, pk);
				sqls = wherePKSplit;
			}

			DBUtil.closeDBResources(null, null, conn);
		} catch (Exception e) {
			LOG.error("intSplit error, parameter:" + parameter, e);
			throw new RuntimeException();
		}

		return sqls;
	}

	private void minmaxNext(JobParameter parameter, Connection conn, BigInteger max, List<BigInteger[]> pkArr) {
		String pk = parameter.getReader().getSplitPk().getPkName();
		String table = parameter.getReader().getTableName();
		Integer splitMax = parameter.getSetting().getSplitMax();
		String sqlNext = String.format("SELECT min( t.%s ) pk_min, max( t.%s ) pk_max FROM ( SELECT %s FROM %s WHERE %s>%s ORDER BY %s LIMIT %s ) t", pk, pk, pk, table, pk, max, pk, splitMax);
		BigInteger[] minmax = pkIntMinMax(parameter, conn, sqlNext);

		if (null != minmax) {
			pkArr.add(minmax);
			LOG.info("minmaxNext,table:" + table + ", size:" + pkArr.size() + ", max:" + minmax[1]);
			minmaxNext(parameter, conn, minmax[1], pkArr);
		} else {

		}
	}

	private BigInteger[] pkIntMinMax(JobParameter parameter, Connection conn, String sql) {
		BigInteger[] minmax = new BigInteger[2];
		try {
			Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = DBUtil.query(stmt, sql);

			if (rs.next()) {
				String minStr = rs.getString("pk_min");
				String maxStr = rs.getString("pk_max");
				if (null != minStr && null != maxStr) {
					BigInteger min = new BigInteger(minStr);
					BigInteger max = new BigInteger(maxStr);

					minmax[0] = min;
					minmax[1] = max;
				} else {
					minmax = null;
				}
			} else {
				minmax = null;
			}

			DBUtil.closeDBResources(rs, stmt, null);
		} catch (SQLException e) {
			LOG.error("pkIntMinMax error, sql:" + sql, e);
			throw new RuntimeException();
		}
		return minmax;
	}

	private List<String> wherePKSplit(List<BigInteger[]> pkArr, String pk) {
		List<String> wheres = new ArrayList<>();
		for (int i = 0; i < pkArr.size() - 1; i++) {
			String where = String.format(" %s >= %s AND %s <= %s", pk, pkArr.get(i)[0], pk, pkArr.get(i)[1]);
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
