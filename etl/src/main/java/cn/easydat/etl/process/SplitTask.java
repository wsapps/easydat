package cn.easydat.etl.process;

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
import cn.easydat.etl.entity.parameter.JobParameterReader;
import cn.easydat.etl.entity.parameter.JobParameterSetting;
import cn.easydat.etl.util.DBUtil;

public class SplitTask {

	private static final Logger LOG = LoggerFactory.getLogger(SplitTask.class);

	public List<String> split(JobParameter parameter) {
		List<String> sqls = null;

		if (null != parameter.getReader().getQuerySql()) {
			sqls = new ArrayList<String>(1);
			sqls.add(parameter.getReader().getQuerySql());
		} else {
			if (null != parameter.getReader().getSplitPk()) {
				if ("int".equals(parameter.getReader().getSplitPk().getPkDataType())) {
					sqls = intSplit(parameter);
				} else if ("varchar".equals(parameter.getReader().getSplitPk().getPkDataType())) {
					varcharSplit(parameter);
				} else {
					throw new RuntimeException("Error Type, pkDataType: " + parameter.getReader().getSplitPk().getPkDataType());
				}
			}
		}
		return sqls;
	}

	private List<String> intSplit(JobParameter parameter) {
		List<String> sqls = null;
		String pk = parameter.getReader().getSplitPk().getPkName();
		String table = parameter.getReader().getTableName();
		String where = parameter.getReader().getWhere();
		String minMaxSql = String.format("SELECT min(%s) min, max(%s) max FROM %s", pk, pk, table);
//		if (null != where) {
//			String whereSql = String.format(" WHERE %s", where);
//			minMaxSql += whereSql;
//		}

		LOG.info("minMaxSql:" + minMaxSql);

		try (Connection conn = DBUtil.getConnection(parameter.getReader().getJdbc());
				Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = DBUtil.query(stmt, minMaxSql);) {

			rs.next();
			String minStr = rs.getString("min");
			String maxStr = rs.getString("max");
			BigInteger min = new BigInteger(minStr);
			BigInteger max = new BigInteger(maxStr);

			LOG.info("split min: {}, max: {}.", min, max);

			BigInteger[] pkArr = doBigIntegerSplit(min, max, parameter.getSetting());
			List<String> wherePKSplit = wherePKSplit(pkArr, pk);
			sqls = splitTask(parameter.getReader(), wherePKSplit);

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
			// 每个任务最大行数为maxNumOfChannel / 5
//			int maxNumOfChannel = setting.getMaxNumOfChannel() / 5;
//			BigInteger range = max.subtract(min);
//			int maxSize = range.divide(BigInteger.valueOf(maxNumOfChannel)).intValue()+1;
//			List<BigInteger> list = new ArrayList<BigInteger>(maxSize);
//			list.add(min);
//			BigInteger num = min;
//			
//			while (num.compareTo(max) < 0) {
//				num = num.add(BigInteger.valueOf(maxNumOfChannel));
//				
//				if (num.compareTo(max) > 0) {
//					num = max;
//				}
//				
//				list.add(num);
//			}
//			
//			result = list.toArray(new BigInteger[] {});

			int expectSliceNumber = setting.getChannel();
			BigInteger endAndStartGap = max.subtract(min);
			BigInteger step = endAndStartGap.divide(BigInteger.valueOf(expectSliceNumber));
			BigInteger remainder = endAndStartGap.remainder(BigInteger.valueOf(expectSliceNumber));

			if (step.compareTo(BigInteger.ZERO) == 0) {
				expectSliceNumber = remainder.intValue();
			}

			result = new BigInteger[expectSliceNumber + 1];
			result[0] = min;
			result[expectSliceNumber] = max;

			BigInteger lowerBound;
			BigInteger upperBound = min;

			for (int i = 1; i < expectSliceNumber; i++) {
				lowerBound = upperBound;
				upperBound = lowerBound.add(step);
				upperBound = upperBound.add((remainder.compareTo(BigInteger.valueOf(i)) >= 0) ? BigInteger.ONE : BigInteger.ZERO);
				result[i] = upperBound;

			}
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

		for (String pkSplit : wherePKSplit) {
			String sqlSplit = sql + " WHERE " + pkSplit;

			if (null != reader.getWhere()) {
				sqlSplit += " AND " + reader.getWhere();
			}

			sqls.add(sqlSplit);
			// LOG.info(sqlSplit);
		}

		return sqls;
	}

	private void varcharSplit(JobParameter parameter) {
		// TODO
	}
}
