package cn.easydat.etl;

import java.io.FileInputStream;
import java.util.List;

import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.easydat.etl.entity.JobParameter;
import cn.easydat.etl.process.ConsumerMain;
import cn.easydat.etl.process.Preprocessing;
import cn.easydat.etl.process.SplitTask;
import cn.easydat.etl.util.DBUtil;

public class Main {
	
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	
	public void startup(JobParameter parameter) {
		init(parameter);
		//split(parameter);
		createProducer(parameter);
	}

	private void createProducer(JobParameter parameter) {
		// TODO Auto-generated method stub
		SplitTask producer = new SplitTask();
		List<String> readerSplitSqlList = producer.split(parameter);

		LOG.info("readerSplitSize size:" + readerSplitSqlList.size());
		
		Preprocessing pre = new Preprocessing();
		pre.startup(parameter);
		
		ConsumerMain consumerMain = new ConsumerMain(parameter, readerSplitSqlList);
		consumerMain.startup();
	}

	private void init(JobParameter parameter) {
//		String path = Main.class.getResource("").getPath();
		//Configuration config = ConfigurationFactory.getInstance().getConfiguration(path);
		String log4jxmlPath = Main.class.getClassLoader().getResource("log4j2.xml").getPath();
		System.out.println("log4jxmlPath:" + log4jxmlPath);
		ConfigurationSource source;
		try {
			source = new ConfigurationSource(new FileInputStream(log4jxmlPath));
			Configurator.initialize(null, source);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		String readerDriver = parameter.getReader().getJdbc().getDriver();
		String writerDriver = parameter.getWriter().getJdbc().getDriver();
		
		DBUtil.loadDriverClass(readerDriver);
		DBUtil.loadDriverClass(writerDriver);
	}
}
