package com.spring.etl.worker.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.scheduling.support.PeriodicTrigger;

import com.spring.etl.worker.application.listener.AddressStepExecutionListener;
import com.spring.etl.worker.domain.inbound.SourceAddressType;
import com.spring.etl.worker.domain.outbound.TargetAddressType;
import com.spring.etl.worker.domain.processor.SourceAddressTypeProcessor;
import com.spring.etl.worker.domain.rowmapper.SourceAddressTypeRowMapper;

@Configuration
@Import(WorkerJobInfraBeanConfig.class)
@EnableBatchProcessing
@EnableBatchIntegration
public class WorkerJobBeanConfig {

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private DataSourceTransactionManager appDataSourceTransactionManager;

	@Autowired
	private JobExplorer jobExplorer;

	@Autowired
	private DataSource dataSource;

	@Bean
	public RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory(
			JobExplorer jobExplorer, JobRepository jobRepository,
			DataSourceTransactionManager appDataSourceTransactionManager) {
		return new RemotePartitioningWorkerStepBuilderFactory(jobRepository, jobExplorer,
				appDataSourceTransactionManager);
	}

	@Bean
	public QueueChannel repliesFromWorkers() {
		return new QueueChannel();
	}

	@Bean
	public IntegrationFlow outboundFlow(KafkaTemplate<Integer, String> kafkaTemplate) {
		return IntegrationFlows.from(repliesFromWorkers())
				.handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic("reply_topic"))
				.route("repliesFromWorkers")
				.get();
	}

	@Bean
	public QueueChannel requestForWorkers() {
		return new QueueChannel();
	}

	@Bean
	public IntegrationFlow inboundFlow(ConsumerFactory<Integer, String> consumerFactory, Step worker_step) {
		return IntegrationFlows
				.from(Kafka.inboundChannelAdapter(consumerFactory, new ConsumerProperties("request_topic")))
				.channel(requestForWorkers())
				.get();
	}

	@Bean(name = PollerMetadata.DEFAULT_POLLER)
	public PollerMetadata defaultPoller() {
		PollerMetadata pollerMetadata = new PollerMetadata();
		pollerMetadata.setTrigger(new PeriodicTrigger(10));
		return pollerMetadata;
	}

	@Bean
	@Scope(scopeName ="step", proxyMode=ScopedProxyMode.TARGET_CLASS)
	public JdbcPagingItemReader<SourceAddressType> pagingItemReader(DataSource dataSource, @Value("#{stepExecutionContext[maxValue]}") Long maxValue, 
			@Value("#{stepExecutionContext[minValue]}") Long minValue) {

		Map<String, Order> sortKeys = new HashMap<>(1);
		sortKeys.put("addr_type_id", Order.ASCENDING);

		PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
		queryProvider.setSelectClause(" addr_type_id, lookup, meaning ");
		queryProvider.setFromClause(" address_type ");
		StringBuilder whereClause = new StringBuilder(" addr_type_id >= ").append(minValue).append(" and addr_type_id <= ").append(maxValue);
		queryProvider.setWhereClause(whereClause.toString());
		queryProvider.setSortKeys(sortKeys);

		JdbcPagingItemReader<SourceAddressType> reader = new JdbcPagingItemReader<>();
		reader.setDataSource(dataSource);
		reader.setFetchSize(1000); // hint to the driver
		reader.setPageSize(10); // controls actual data fetched from DB 
		reader.setRowMapper(new SourceAddressTypeRowMapper());
		reader.setQueryProvider(queryProvider);
		return reader;
	}

	@Bean
	@Scope(scopeName ="step", proxyMode=ScopedProxyMode.TARGET_CLASS)
	public SourceAddressTypeProcessor sourceAddressTypeProcessor() {
		return new SourceAddressTypeProcessor();
	}

	@Bean
	@Scope(scopeName ="step", proxyMode=ScopedProxyMode.TARGET_CLASS)
	public JdbcBatchItemWriter<TargetAddressType> customerItemWriter(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<TargetAddressType>().dataSource(dataSource)
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO public.target_address_type " + " (lookup, meaning) " + " VALUES(:lookup, :meaning);")
				.build();
	}
	
	@Bean
	@Scope(scopeName ="step", proxyMode=ScopedProxyMode.TARGET_CLASS)
	public AddressStepExecutionListener addressStepExecutionListener() {
		return new AddressStepExecutionListener();
	}

	@Bean
	public Step worker_step(RemotePartitioningWorkerStepBuilderFactory remotePartitioningWorkerStepBuilderFactory,
			JdbcPagingItemReader<SourceAddressType> pagingItemReader,
			SourceAddressTypeProcessor sourceAddressTypeProcessor, ItemWriter<TargetAddressType> customerItemWriter, AddressStepExecutionListener addressStepExecutionListener) {
		return remotePartitioningWorkerStepBuilderFactory.get("worker_step")
				.inputChannel(requestForWorkers())
				.outputChannel(repliesFromWorkers())
				.<SourceAddressType, TargetAddressType>chunk(10) // commit interval
				.reader(pagingItemReader)
				.processor(sourceAddressTypeProcessor)
				.writer(customerItemWriter)
				// .listener(addressStepExecutionListener)
				.build();
	}

	@Bean
	public Job workerJob(JobBuilderFactory jobBuilderFactory, Step worker_step) {
		return jobBuilderFactory.get("worker_job")
				.incrementer(new RunIdIncrementer())
				.flow(worker_step)
				.end()
				.build();
	}
}
