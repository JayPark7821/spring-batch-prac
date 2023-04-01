package kr.jay.batch.part4;


import static org.assertj.core.api.Assertions.*;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import kr.jay.batch.TestConfiguration;

@SpringBatchTest
@ContextConfiguration(classes = {UserConfiguration.class, TestConfiguration.class})
class UserConfigurationTest {

	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@Autowired
	private UserRepository userRepository;

	@Test
	void test() throws Exception {
	    //when
		final JobExecution jobExecution = jobLauncherTestUtils.launchJob();

		//then
		int size = userRepository.findAllByUpdateDate(LocalDate.now()).size();
		assertThat(jobExecution.getStepExecutions().stream().filter(x -> x.getStepName().equals("userLevelUpStep"))
			.mapToInt(StepExecution::getWriteCount)
			.sum())
			.isEqualTo(size)
			.isEqualTo(300);

		assertThat(userRepository.count()).isEqualTo(400);


	}
}