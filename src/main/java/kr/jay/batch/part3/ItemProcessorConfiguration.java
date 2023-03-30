package kr.jay.batch.part3;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class ItemProcessorConfiguration {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;

	public ItemProcessorConfiguration(final JobBuilderFactory jobBuilderFactory,
		final StepBuilderFactory stepBuilderFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
	}

	@Bean
	public Job itemProcessorJob() {
		return this.jobBuilderFactory.get("itemProcessorJob")
			.incrementer(new RunIdIncrementer())
			.start(this.itemProcessorStep())
			.build();
	}

	@Bean
	public Step itemProcessorStep() {
		return this.stepBuilderFactory.get("itemProcessorStep")
			.<Person, Person>chunk(10)
			.reader(itemReader())
			.processor(itemProcessor())
			.writer(itemWriter())
			.build();
	}

	private ItemWriter<Person> itemWriter() {
		return items -> items.forEach(x -> log.info("person: {}", x.getId()));
	}

	private ItemProcessor<? super Person, ? extends Person> itemProcessor() {
		return item -> {
			if(item.getId() % 2 == 0) {
				return item;
			}
			return null;
		};
	}

	private ItemReader<Person> itemReader() {
		return new CustomItemReader<>(getItems());
	}

	private List<Person> getItems() {
		return IntStream.range(0,10)
			.mapToObj(i -> new Person(i + 1, "test name" + i, "test age", "test address"))
			.collect(Collectors.toList());
	}
}
