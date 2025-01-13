package cn.easydat.etl.job;

import org.noear.solon.annotation.Controller;
import org.noear.solon.annotation.Get;
import org.noear.solon.annotation.Inject;
import org.noear.solon.annotation.Mapping;
import org.noear.solon.annotation.Path;
import org.noear.solon.annotation.Post;
import org.noear.solon.validation.annotation.NotNull;

import cn.easydat.etl.job.service.JobService;

@Mapping("job")
@Controller
public class JobMain {


	@Inject
	private JobService jobService;

	@Post
	@Mapping("run/{jobId}")
	public Long run(@NotNull @Path("jobId") Integer jobId) {
		Long processNo = jobService.createJobProcess(jobId);
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

}
