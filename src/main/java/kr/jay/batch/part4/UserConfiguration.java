package kr.jay.batch.part4;

import javax.persistence.EntityManagerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class UserConfiguration {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final UserRepository userRepository;
	private final EntityManagerFactory emf;
	public UserConfiguration(final JobBuilderFactory jobBuilderFactory,
		final StepBuilderFactory stepBuilderFactory, final UserRepository userRepository, final EntityManagerFactory emf) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.userRepository = userRepository;
		this.emf = emf;
	}

	@Bean
	public Job userJob() throws Exception {
		return this.jobBuilderFactory.get("userJob")
			.incrementer(new RunIdIncrementer())
			.start(this.saveUserStep())
			.next(this.userLevelUpStep())
			.build();
	}

	@Bean
	public Step saveUserStep() {
		return this.stepBuilderFactory.get("saveUserStep")
			.tasklet(new SaveUserTasklet(userRepository))
			.build();
	}

	@Bean
	public Step userLevelUpStep() throws Exception {
		return this.stepBuilderFactory.get("userLevelUpStep")
			.<User, User>chunk(100)
			.reader(itemReader())
			.processor(itemProcessor())
			.writer(itemWriter())
			.build();
	}

	private ItemWriter<? super User> itemWriter() {
		return users ->
			users.forEach(user -> {
				user.levelUp();
				userRepository.save(user);
			});
	}

	private ItemProcessor<? super User, ? extends User> itemProcessor() {
		return user ->{
			if (user.availableLevelUp()) {
				return user;
			}
			return null;
		};
	}

	private ItemReader<? extends User> itemReader() throws Exception {
		final JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
			.queryString("select u from User u ")
			.entityManagerFactory(emf)
			.pageSize(100)
			.name("userItemReader")
			.build();

		itemReader.afterPropertiesSet();
		return itemReader;
	}
}
