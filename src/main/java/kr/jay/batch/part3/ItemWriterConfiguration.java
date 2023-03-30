package kr.jay.batch.part3;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class ItemWriterConfiguration {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final DataSource dataSource;
	private final EntityManagerFactory emf;


	public ItemWriterConfiguration(final JobBuilderFactory jobBuilderFactory,
		final StepBuilderFactory stepBuilderFactory, final DataSource dataSource, final EntityManagerFactory emf) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.dataSource = dataSource;
		this.emf = emf;
	}

	@Bean
	public Job itemWriterJob() throws Exception {
		return this.jobBuilderFactory.get("itemWriterJob")
			.incrementer(new RunIdIncrementer())
			.start(this.csvItemWriterStep())
			// .next(this.jdbcBatchItemWriterStep())
			.next(this.jpaItemWriterStep())
			.build();
	}

	@Bean
	public Step csvItemWriterStep() throws Exception {
		return this.stepBuilderFactory.get("csvItemWriterStep")
			.<Person, Person>chunk(10)
			.reader(itemReader())
			.writer(csvFileItemWriter())
			.build();
	}

	@Bean
	public Step jdbcBatchItemWriterStep(){
		return stepBuilderFactory.get("jdbcBatchItemWriterStep")
			.<Person, Person>chunk(10)
			.reader(itemReader())
			.writer(jdbcBatchItemWriter())
			.build();
	}

	@Bean
	public Step jpaItemWriterStep() throws Exception {
		return stepBuilderFactory.get("jpaItemWriterStep")
			.<Person, Person>chunk(10)
			.reader(itemReader())
			.writer(jpaItemWriter())
			.build();
	}

	private ItemWriter<Person> jpaItemWriter() throws Exception {
		final JpaItemWriter<Person> itemWriter = new JpaItemWriterBuilder<Person>()
			.entityManagerFactory(emf)
			.usePersist(true) // default: false
			.build();

		itemWriter.afterPropertiesSet();

		return itemWriter;
	}

	private ItemWriter<Person> jdbcBatchItemWriter() {
		final JdbcBatchItemWriter<Person> itemWriter = new JdbcBatchItemWriterBuilder<Person>()
			.dataSource(dataSource)
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql("insert into person(name, age, address) values (:name, :age, :address)")
			.build();

		itemWriter.afterPropertiesSet();
		return itemWriter;

	}

	private ItemWriter<Person> csvFileItemWriter() throws Exception {
		final BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] {"id", "name", "age", "address"});

		final DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");
		lineAggregator.setFieldExtractor(fieldExtractor);

		final FlatFileItemWriter<Person> itemWriter = new FlatFileItemWriterBuilder<Person>()
			.name("csvFileItemWriter")
			.encoding("UTF-8")
			.resource(new FileSystemResource("output/test-output.csv"))
			.lineAggregator(lineAggregator)
			.headerCallback(writer -> writer.write("id, 이름, 나이, 거주지"))
			.footerCallback(writer -> writer.write("-------------------\n"))
			.append(true)
			.build();

		itemWriter.afterPropertiesSet();
		return itemWriter;
	}

	private ItemReader<Person> itemReader() {
		return new CustomItemReader<>(getItems());
	}

	private List<Person> getItems() {
		return IntStream.range(0,100)
			.mapToObj(i -> new Person("test name" + i, "test age", "test address"))
			.collect(Collectors.toList());
	}
}
