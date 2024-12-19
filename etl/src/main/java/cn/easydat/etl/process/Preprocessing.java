package cn.easydat.etl.process;

import java.sql.Connection;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.util.DBUtil;

public class Preprocessing {

	private static final Logger LOG = LoggerFactory.getLogger(Preprocessing.class);

	public void startup(JobParameter parameter) {
		try {
			Connection conn = DBUtil.getConnection(parameter.getWriter().getJdbc());
			Statement stmt = conn.createStatement();
			stmt.execute(parameter.getWriter().getPreSql());
		} catch (Exception e) {
			LOG.error(parameter.getWriter().getPreSql(), e);
		}

	}

}
