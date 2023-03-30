package kr.jay.batch.part3;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.annotation.Transactional;

@SpringBatchTest
@ContextConfiguration(classes = {SavePersonConfiguration.class, TestConfiguration.class})
class SavePersonConfigurationTest {

	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@Autowired
	private PersonRepository repository;

	@AfterEach
	void tearDown() {
		repository.deleteAll();
	}

	@Test
	void test_step(){
	    //when
		final JobExecution jobExecution = jobLauncherTestUtils.launchStep("savePersonStep");

		//given
		assertThat(jobExecution.getStepExecutions()
			.stream()
			.mapToInt(StepExecution::getWriteCount)
			.sum())
			.isEqualTo(4)
			.isEqualTo(repository.count());
	    //then

	}
	@Test
	void  test_allow_duplicate() throws Exception {
		final JobParameters jobParameters = new JobParametersBuilder()
			.addString("allow_duplicate", "false")
			.toJobParameters();

		final JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

		assertThat(jobExecution.getStepExecutions()
			.stream()
			.mapToInt(StepExecution::getWriteCount)
			.sum())
			.isEqualTo(4)
			.isEqualTo(repository.count());
	}

	@Test
	void  test_not_allow_duplicate() throws Exception {
		final JobParameters jobParameters = new JobParametersBuilder()
			.addString("allow_duplicate", "true")
			.toJobParameters();

		final JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

		assertThat(jobExecution.getStepExecutions()
			.stream()
			.mapToInt(StepExecution::getWriteCount)
			.sum())
			.isEqualTo(100)
			.isEqualTo(repository.count());

	}
}