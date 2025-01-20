package cn.easydat.etl.job;

import java.math.BigInteger;

import org.noear.solon.annotation.Controller;
import org.noear.solon.annotation.Get;
import org.noear.solon.annotation.Inject;
import org.noear.solon.annotation.Mapping;
import org.noear.solon.annotation.Path;
import org.noear.solon.annotation.Post;
import org.noear.solon.validation.annotation.NotNull;

import cn.easydat.etl.job.service.JobService;
import cn.easydat.etl.job.service.TestService;

@Mapping("job")
@Controller
public class JobMain {

	@Inject
	private JobService jobService;
	
	@Inject
	private TestService testService;

	@Post
	@Mapping("run/{jobId}")
	public BigInteger run(@NotNull @Path("jobId") Integer jobId) {
		BigInteger processNo = jobService.runJob(jobId);
		return processNo;
	}
	
	@Get
	@Mapping("executeTask")
	public String executeTask() {
		
		Thread thread = new Thread(() -> {
			Integer taskId = 1;
			
			while (null != taskId) {
				taskId = jobService.executeTask();
			}
		});
		thread.start();
		
		return "execute";
	}
	
	@Get
	@Mapping("createAndRunOneJobTask/{processNo}")
	public String createAndExecuteOneJobTask(@NotNull @Path("processNo") BigInteger processNo) {
		
		Thread thread = new Thread(() -> {
			jobService.createAndRunJobTask(processNo);
		});
		thread.start();
		
		return "execute";
	}

}
