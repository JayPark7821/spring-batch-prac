package kr.jay.batch.part3;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.batch.core.annotation.BeforeStep;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SavePersonListener {

	public static class SavePersonStepExecutionListener{

		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) {
			log.info("beforeStep");
		}

		@AfterStep
		public ExitStatus afterStep(final StepExecution stepExecution) {
			log.info("afterStep: {}", stepExecution.getWriteCount());
			return stepExecution.getExitStatus();
		}
	}
	public static class SavePersonJobExecutionListener implements JobExecutionListener {

		@Override
		public void beforeJob(final JobExecution jobExecution) {
			log.info("beforeJob");
		}

		@Override
		public void afterJob(final JobExecution jobExecution) {
			final int sum = jobExecution.getStepExecutions().stream()
				.mapToInt(StepExecution::getWriteCount)
				.sum();

			log.info("afterJob: {}", sum);
		}
	}

	public static class SavePersonAnnotationJobExecutionListener{
		@BeforeJob
		public void beforeJob(final JobExecution jobExecution) {
			log.info("annotation beforeJob");
		}

		@AfterJob
		public void afterJob(final JobExecution jobExecution) {
			final int sum = jobExecution.getStepExecutions().stream()
				.mapToInt(StepExecution::getWriteCount)
				.sum();

			log.info("annotation afterJob: {}", sum);
		}
	}
}
