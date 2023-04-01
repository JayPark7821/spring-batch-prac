package kr.jay.batch.part5;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

import io.micrometer.core.instrument.util.StringUtils;

public class JobParametersDecider implements JobExecutionDecider {

	public static final FlowExecutionStatus CONTINUE = new FlowExecutionStatus("CONTINUE");
	private final String key;

	public JobParametersDecider(final String key) {
		this.key = key;
	}

	@Override
	public FlowExecutionStatus decide(final JobExecution jobExecution, final StepExecution stepExecution) {
		final String value = jobExecution.getJobParameters().getString(key);
		if (StringUtils.isEmpty(value)) {
			return FlowExecutionStatus.COMPLETED;
		}
		return CONTINUE;
	}
}
