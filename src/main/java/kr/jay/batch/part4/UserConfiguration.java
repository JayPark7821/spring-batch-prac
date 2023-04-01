package kr.jay.batch.part4;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import kr.jay.batch.part5.JobParametersDecider;
import kr.jay.batch.part5.OrderStatistics;
import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class UserConfiguration {

	private final int CHUNK = 100;
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final UserRepository userRepository;
	private final EntityManagerFactory emf;
	private final DataSource dataSource;

	public UserConfiguration(final JobBuilderFactory jobBuilderFactory,
		final StepBuilderFactory stepBuilderFactory, final UserRepository userRepository,
		final EntityManagerFactory emf, final DataSource dataSource) {
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.userRepository = userRepository;
		this.emf = emf;
		this.dataSource = dataSource;
	}

	@Bean
	public Job userJob() throws Exception {
		return this.jobBuilderFactory.get("userJob")
			.incrementer(new RunIdIncrementer())
			.start(this.saveUserStep())
			.next(this.userLevelUpStep())
			.listener(new LevelUpJobExecutionListener(userRepository))
			.next(new JobParametersDecider("date"))
			.on(JobParametersDecider.CONTINUE.getName())
			.to(this.orderStatisticsStep(null))
			.build()
			.build();
	}

	@Bean
	@JobScope
	public Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
		return this.stepBuilderFactory.get("orderStatisticsStep")
			.<OrderStatistics, OrderStatistics>chunk(CHUNK)
			.reader(orderStatisTicsItemReader(date))
			.writer(orderStatisticsItemWriter(date))
			.build();
	}

	private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(final String date) throws Exception {
		final YearMonth yearMonth = YearMonth.parse(date);

		final String fileName = yearMonth.getYear() + "년_" + yearMonth.getMonthValue() + "월_일별_주문_금액.csv";

		final BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[]{"amount", "date"});

		final DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");
		lineAggregator.setFieldExtractor(fieldExtractor);

		final FlatFileItemWriter<OrderStatistics> itemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
			.resource(new FileSystemResource("output/" + fileName))
			.lineAggregator(lineAggregator)
			.name("orderStatisticsItemWriter")
			.encoding("UTF-8")
			.headerCallback(writer -> writer.write("amount,date"))
			.build();

		itemWriter.afterPropertiesSet();

		return itemWriter;

	}

	private ItemReader<? extends OrderStatistics> orderStatisTicsItemReader(final String date) throws Exception {
		final YearMonth yearMonth = YearMonth.parse(date);
		Map<String, Object > params = new HashMap<>();
		params.put("startDate", yearMonth.atDay(1));
		params.put("endDate", yearMonth.atEndOfMonth());

		Map<String, Order> sortKey = new HashMap<>();
		sortKey.put("created_date", Order.ASCENDING);

		final JdbcPagingItemReader<OrderStatistics> itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
			.dataSource(this.dataSource)
			.rowMapper((resultSet, i) -> OrderStatistics.builder()
				.amount(resultSet.getString(1))
				.date(LocalDate.parse(resultSet.getString(2), DateTimeFormatter.ISO_DATE))
				.build())
			.pageSize(CHUNK)
			.name("orderStatisticsItemReader")
			.selectClause("sum(amount), created_date")
			.fromClause("orders")
			.whereClause("created_date >= :startDate and created_date <= :endDate")
			.groupClause("created_date")
			.parameterValues(params)
			.sortKeys(sortKey)
			.build();

		itemReader.afterPropertiesSet();

		return itemReader;

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
			.<User, User>chunk(CHUNK)
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
			.pageSize(CHUNK)
			.name("userItemReader")
			.build();

		itemReader.afterPropertiesSet();
		return itemReader;
	}
}
