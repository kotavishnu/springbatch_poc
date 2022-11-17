package com.spring.etl.manager.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.batch.core.configuration.ListableJobLocator;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.support.DatabaseType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.etl.manager.jacksonutils.JobExecutionMixin;
import com.spring.etl.manager.jacksonutils.StepExecutionMixin;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Configuration
@PropertySource("classpath:application.properties")
public class ManagerJobInfraBeanConfig {

	@Bean
	@Primary
	public DataSource dataSource() {
		HikariConfig config = new HikariConfig();
		config.setDriverClassName("org.postgresql.Driver");
		config.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
		config.setUsername("postgres");
		config.setPassword("password");
		config.setConnectionTimeout(10000L);
		config.setIdleTimeout(20000L);
		config.setMaxLifetime(2000000L);
		config.setMaximumPoolSize(8);
		config.setMinimumIdle(1000);
		config.addDataSourceProperty("useServerPrepStmts", Boolean.TRUE.toString());
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", 250);
		config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
		config.addDataSourceProperty("useLocalSessionState", Boolean.TRUE.toString());
		config.addDataSourceProperty("rewriteBatchedStatements", Boolean.TRUE.toString());
		config.addDataSourceProperty("cacheResultSetMetadata", Boolean.TRUE.toString());
		config.addDataSourceProperty("cacheServerConfiguration", Boolean.TRUE.toString());
		config.addDataSourceProperty("elideSetAutoCommits", Boolean.TRUE.toString());
		config.addDataSourceProperty("maintainTimeStats", Boolean.FALSE.toString());
		config.addDataSourceProperty("netTimeoutForStreamingResults", 0);
		return new HikariDataSource(config);
	}

	@Bean
	public DataSourceTransactionManager appDataSourceTransactionManager(DataSource dataSource) {
		return new DataSourceTransactionManager(dataSource);
	}

	@Bean
	public JdbcOperations jdbcOperations(DataSource dataSource) {
		JdbcOperations jdbcOperations = new JdbcTemplate(dataSource);
		return jdbcOperations;
	}



	@Bean
	public JobExplorer jobExplorer(DataSource dataSource, JdbcOperations jdbcOperations) throws Exception {
		
		ObjectMapper mapper = new ObjectMapper();
		mapper.addMixIn(org.springframework.batch.core.StepExecution.class, StepExecutionMixin.class);
		mapper.addMixIn(org.springframework.batch.core.JobExecution.class, JobExecutionMixin.class);
		
		Jackson2ExecutionContextStringSerializer jackson2ExecutionContextStringSerializer = new Jackson2ExecutionContextStringSerializer();
		jackson2ExecutionContextStringSerializer.setObjectMapper(mapper);

		JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
		jobExplorerFactoryBean.setDataSource(dataSource);
		jobExplorerFactoryBean.setJdbcOperations(jdbcOperations);
		jobExplorerFactoryBean.setSerializer(jackson2ExecutionContextStringSerializer);
		return jobExplorerFactoryBean.getObject();
	}

	@Bean
	public JobRepository jobRepository(DataSource dataSource, JdbcOperations jdbcOperations,
			DataSourceTransactionManager appDataSourceTransactionManager) throws Exception {
		JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
		jobRepositoryFactoryBean.setDatabaseType(DatabaseType.POSTGRES.name());
		jobRepositoryFactoryBean.setDataSource(dataSource);
		jobRepositoryFactoryBean.setJdbcOperations(jdbcOperations);
		jobRepositoryFactoryBean.setTransactionManager(appDataSourceTransactionManager);
		jobRepositoryFactoryBean.setValidateTransactionState(Boolean.TRUE);
		jobRepositoryFactoryBean.setIsolationLevelForCreate("ISOLATION_READ_COMMITTED");
		return jobRepositoryFactoryBean.getObject();
	}

	@Bean
	public ThreadPoolTaskExecutor jobOperatorExecutor() {
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(6);
		threadPoolTaskExecutor.setMaxPoolSize(8);
		threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
		return threadPoolTaskExecutor;
	}

	@Bean
	public JobLauncher jobLauncher(JobRepository jobRepository, ThreadPoolTaskExecutor jobOperatorExecutor)
			throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		jobLauncher.setTaskExecutor(jobOperatorExecutor);
		jobLauncher.afterPropertiesSet();
		return jobLauncher;
	}

	@Bean
	public JobBuilderFactory jobBuilderFactory(JobRepository jobRepository) {
		JobBuilderFactory jobBuilderFactory = new JobBuilderFactory(jobRepository);
		return jobBuilderFactory;
	}

	@Bean
	public ProducerFactory<Integer, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // bootstrapAddress holds address of
																					// kafka server
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public ConsumerFactory<Integer, String> consumerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // bootstrapAddress holds address of
																					// kafka server
		configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "manager-group");
		configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		return new DefaultKafkaConsumerFactory<Integer, String>(configProps);
	}

	@Bean
	public KafkaTemplate<Integer, String> kafkaTemplate(ProducerFactory<Integer, String> producerFactory) {
		return new KafkaTemplate<Integer, String>(producerFactory);
	}

	@Bean
	public ListableJobLocator listableJObLocator() {
		return new MapJobRegistry();
	}

}
