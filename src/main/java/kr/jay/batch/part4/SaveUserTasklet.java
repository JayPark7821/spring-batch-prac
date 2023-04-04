package kr.jay.batch.part4;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import kr.jay.batch.part5.Orders;

public class SaveUserTasklet implements Tasklet {

	private final int SIZE = 10_000;
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
		final List<User> users = IntStream.range(0, SIZE)
			.mapToObj(i -> new User("test userName " + i,
				Collections.singletonList(Orders.builder()
					.amount(1_000)
					.createdDate(LocalDate.of(2020, 11, 1))
					.itemName("iteam" + i)
					.build())))
			.collect(Collectors.toList());

		users.addAll(IntStream.range(0, SIZE)
			.mapToObj(i -> new User("test userName " + i, Collections.singletonList(Orders.builder()
				.amount(200_000)
				.createdDate(LocalDate.of(2020, 12, 2))
				.itemName("iteam" + i)
				.build())))
			.collect(Collectors.toList()));

		users.addAll(IntStream.range(0, SIZE)
			.mapToObj(i -> new User("test userName " + i, Collections.singletonList(Orders.builder()
				.amount(300_000)
				.createdDate(LocalDate.of(2020, 11, 3))
				.itemName("iteam" + i)
				.build())))
			.collect(Collectors.toList()));

		users.addAll(IntStream.range(0, SIZE)
			.mapToObj(i -> new User("test userName " + i, Collections.singletonList(Orders.builder()
				.amount(500_000)
				.createdDate(LocalDate.of(2020, 11, 4))
				.itemName("iteam" + i)
				.build())))
			.collect(Collectors.toList()));

		return users;
	}
}
