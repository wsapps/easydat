package cn.easydat.etl.job.common;

import cn.easydat.etl.job.service.SimpleConnectionPool;

public class Container {

	private static final SimpleConnectionPool CONNECTION_POOL = new SimpleConnectionPool(50);
	
	public static SimpleConnectionPool getConnectionPool() {
		return CONNECTION_POOL;
	}
}
