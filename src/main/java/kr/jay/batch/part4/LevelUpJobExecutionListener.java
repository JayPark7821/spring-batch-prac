package kr.jay.batch.part4;

import java.time.LocalDate;
import java.util.List;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LevelUpJobExecutionListener implements JobExecutionListener {

	private final UserRepository userRepository;

	public LevelUpJobExecutionListener(final UserRepository userRepository) {
		this.userRepository = userRepository;
	}

	@Override
	public void beforeJob(final JobExecution jobExecution) {

	}

	@Override
	public void afterJob(final JobExecution jobExecution) {
		final List<User> users = userRepository.findAllByUpdateDate(LocalDate.now());

		long time = jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime();
		log.info("회원등급 업데이트 배치 프로그램");
		log.info("------------------------");
		log.info("총 데이터 처리 {} 건, 처리 시간 {}ms", users.size(), time);

	}
}
