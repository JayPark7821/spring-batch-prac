package kr.jay.batch.part4;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class SaveUserTasklet implements Tasklet {

	private final UserRepository repository;

	public SaveUserTasklet(final UserRepository repository) {
		this.repository = repository;
	}

	@Override
	public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
		List<User> users = createUsers();
		Collections.shuffle(users);
		repository.saveAll(users);

		return RepeatStatus.FINISHED;
	}

	private List<User> createUsers() {
		final List<User> users = IntStream.range(0, 100)
			.mapToObj(i -> new User("test userName " + i, 1_000))
			.collect(Collectors.toList());

		users.addAll(IntStream.range(0, 100)
			.mapToObj(i -> new User("test userName " + i, 200_000))
			.collect(Collectors.toList()));

		users.addAll(IntStream.range(0, 100)
			.mapToObj(i -> new User("test userName " + i, 300_000))
			.collect(Collectors.toList()));

		users.addAll(IntStream.range(0, 100)
			.mapToObj(i -> new User("test userName " + i, 500_000))
			.collect(Collectors.toList()));

		return users;
	}
}
