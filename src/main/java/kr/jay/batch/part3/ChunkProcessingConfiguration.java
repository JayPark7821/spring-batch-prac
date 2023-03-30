package kr.jay.batch.part3;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class ChunkProcessingConfiguration {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;

	public ChunkProcessingConfiguration(final JobBuilderFactory jobBuilderFactory,
		final StepBuilderFactory stepBuilderFactory) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
	}

	@Bean
	public Job chunkProcessingJob() {
		return jobBuilderFactory.get("chunkProcessingJob")
			.incrementer(new RunIdIncrementer())
			.start(this.taskBaseStep())
			.next(this.chunkBaseStep(null))
			.build();
	}

	@Bean
	@JobScope
	public Step chunkBaseStep(@Value("#{jobParameters[chunkSize]}") String chunkSize) {
		return stepBuilderFactory.get("chunkBaseStep")
			.<String, String>chunk(StringUtils.isNotEmpty(chunkSize) ? Integer.parseInt(chunkSize) : 10)
			.reader(iteamReader())
			.processor(itemProcessor())
			.writer(itemWriter())
			.build();
	}

	private ItemWriter<String> itemWriter() {
		return items -> log.info("chunk item size : {}", items.size());
		// return items -> items.forEach(log::info);
	}

	private ItemProcessor<String, String> itemProcessor() {
		return item -> item + ", Spring Batch";
	}

	private ItemReader<String> iteamReader() {
		return new ListItemReader<>(getItems());
	}

	@Bean
	public Step taskBaseStep() {
		return stepBuilderFactory.get("taskBaseStep")
			.tasklet(this.tasklet(null))
			.build();
	}

	@Bean
	@StepScope
	public Tasklet tasklet(@Value("#{jobParameters[chunkSize]}") String value) {
		List<String> items = getItems();
		return ((contribution, chunkContext) -> {
			final StepExecution stepExecution = contribution.getStepExecution();
			final JobParameters jobParameters = stepExecution.getJobParameters();
			// final String values = jobParameters.getString("chunkSize", "10");
			int chunkSize = StringUtils.isNotEmpty(value) ? Integer.parseInt(value) : 10;

			int fromIndex = stepExecution.getReadCount();
			int toIndex = fromIndex + chunkSize;

			if (fromIndex >= items.size()) {
				return RepeatStatus.FINISHED;
			}

			final List<String> subList = items.subList(fromIndex, toIndex);
			log.info("task item size : {}", subList.size());
			stepExecution.setReadCount(toIndex);

			return RepeatStatus.CONTINUABLE;
		});
	}

	private List<String> getItems() {
		return  IntStream.range(0, 100)
			.mapToObj(i -> i + " Hello")
			.collect(Collectors.toList());
	}
}
