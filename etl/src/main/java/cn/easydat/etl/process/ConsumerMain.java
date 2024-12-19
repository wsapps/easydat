package cn.easydat.etl.process;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;

public class ConsumerMain {

	private static final Logger LOG = LoggerFactory.getLogger(ConsumerMain.class);

	private JobParameter parameter;
	private List<String> readerSplitSqlList;
//	private List<MetaData> metaDatas;

	public ConsumerMain(JobParameter parameter, List<String> readerSplitSqlList) {
		super();
		this.parameter = parameter;
		this.readerSplitSqlList = readerSplitSqlList;
	}

	public void startup() {
		int channel = parameter.getSetting().getChannel();
		int size = readerSplitSqlList.size();
		ExecutorService executorService = new ThreadPoolExecutor(channel, size, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(size));

		for (int i = 0; i < size; i++) {
//			final int num = i;
//			executorService.submit(() -> {
//				consumerHandler(parameter, readerSplitSqlList.get(num));
//			});
			
			executorService.submit(new ConsumerRunnable(parameter, readerSplitSqlList.get(i), i));
		}

		executorService.shutdown();
		
		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.error("", e);
			}
		}
	}

//	private void consumerHandler(JobParameter parameter, String readerSql) {
//		try {
//			Connection readerConn = DBUtil.getConnection(parameter.getReader().getJdbc());
//			Statement stmt = readerConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//			ResultSet rs = stmt.executeQuery(readerSql);
//			List<MetaData> metaDataList = getMetaDatas(rs);
//			List<Object[]> datas = new ArrayList<Object[]>();
//			
//			while (rs.next()) {
//				int fieldSize = metaDataList.size();
//				Object [] data = new Object[fieldSize];
//				for(int i = 0; i < fieldSize; i++) {
//					data[i] = rs.getObject(i);
//				}
//				
//				datas.add(data);
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//	private List<MetaData> getMetaDatas(ResultSet rs) {
//		if (null == metaDatas) {
//			loadMetaDatas(rs);
//		}
//		return metaDatas;
//	}
//
//	private synchronized void loadMetaDatas(ResultSet rs) {
//		try {
//			ResultSetMetaData metaData = rs.getMetaData();
//			int columnCount = metaData.getColumnCount();
//
//			List<MetaData> metaDataList = new ArrayList<MetaData>(columnCount);
//
//			for (int i = 0; i < columnCount; i++) {
//				MetaData md = new MetaData(metaData.getColumnLabel(i), metaData.getColumnName(i), metaData.getColumnTypeName(i), metaData.getColumnType(i));
//				metaDataList.add(md);
//			}
//
//			this.metaDatas = metaDataList;
//		} catch (SQLException e) {
//			LOG.error("", e);
//			throw new RuntimeException(e);
//		}
//	}

}
