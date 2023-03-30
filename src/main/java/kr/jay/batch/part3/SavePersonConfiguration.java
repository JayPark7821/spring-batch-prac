package kr.jay.batch.part3;

import javax.persistence.EntityManagerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class SavePersonConfiguration {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final EntityManagerFactory emf;

	public SavePersonConfiguration(final JobBuilderFactory jobBuilderFactory,
		final StepBuilderFactory stepBuilderFactory, final EntityManagerFactory emf) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.emf = emf;
	}

	@Bean
	public Job savePersonJob() throws Exception {
		return this.jobBuilderFactory.get("savePersonJob")
			.incrementer(new RunIdIncrementer())
			.start(this.savePersonStep(null))
			.listener(new SavePersonListener.SavePersonJobExecutionListener())
			.listener(new SavePersonListener.SavePersonAnnotationJobExecutionListener())
			.build();
	}

	@Bean
	@JobScope
	public Step savePersonStep(@Value("#{jobParameters[allow_duplicate]}") String allowDuplicate) throws Exception {
		return this.stepBuilderFactory.get("savePersonStep")
			.<Person, Person>chunk(10)
			.reader(itemReader())
			.processor(new DuplicateValidationProcessor<>(Person::getName, Boolean.parseBoolean(allowDuplicate)))
			.writer(iteamWriter())
			.listener(new SavePersonListener.SavePersonStepExecutionListener())
			.faultTolerant()
			.skip(NotFoundNameException.class)
			.skipLimit(3)
			.build();
	}



	private ItemWriter<? super Person> iteamWriter() throws Exception {
	//	return items -> items.forEach(x-> log.info("person: {}", x.getName()));
		final JpaItemWriter<Person> itemWriter = new JpaItemWriterBuilder<Person>()
			.entityManagerFactory(emf)
			.build();

		ItemWriter<Person> logItemWriter = items -> log.info("person.size() : {}" , items.size());

		final CompositeItemWriter<Person> finalItemWriter = new CompositeItemWriterBuilder<Person>()
			.delegates(itemWriter, logItemWriter)
			.build();
		finalItemWriter.afterPropertiesSet();

		return finalItemWriter;
	}

	private ItemReader<? extends Person> itemReader() throws Exception {
		final DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
		final DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames("name", "age", "address");
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSet -> {
			final String name = fieldSet.readString(0);
			final String age = fieldSet.readString(1);
			final String address = fieldSet.readString(2);
			return new Person(name, age, address);
		});
		final FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
			.name("savePersonItemReader")
			.encoding("UTF-8")
			.linesToSkip(1)
			.resource(new ClassPathResource("person.csv"))
			.lineMapper(lineMapper)
			.build();

		itemReader.afterPropertiesSet();
		return itemReader;
	}
}
