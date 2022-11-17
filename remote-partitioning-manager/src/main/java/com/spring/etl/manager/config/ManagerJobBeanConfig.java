package com.spring.etl.manager.config;

import java.util.function.Function;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.messaging.Message;

import com.spring.etl.manager.partition.ColumnRangePartitioner;

@Configuration
@Import(ManagerJobInfraBeanConfig.class)
@EnableBatchProcessing
@EnableBatchIntegration
public class ManagerJobBeanConfig {

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private DataSourceTransactionManager appDataSourceTransactionManager;

	@Autowired
	private JobExplorer jobExplorer;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Value(" select addr_type_id from address_type ")
	private String table;

	@Value("addr_type_id")
	private String column;

	@Bean
	public RemotePartitioningManagerStepBuilderFactory remotePartitioningManagerStepBuilderFactory() {
		return new RemotePartitioningManagerStepBuilderFactory(jobRepository, this.jobExplorer,
				appDataSourceTransactionManager);
	}

	@Bean
	public String column() {
		return new String(column);
	}
	
	@Bean
	public String table() {
		return new String(table);
	}
	
	@Bean
	@StepScope
	public ColumnRangePartitioner columnRangePartitioner(JdbcOperations jdbcOperations, String table, String column) {
		return new ColumnRangePartitioner(jdbcOperations, table, column);
	}

	@Bean
	public DirectChannel requestForWorkers() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow outboundFlow(KafkaTemplate<Integer, String> kafkaTemplate) {
        KafkaProducerMessageHandler<Integer, String> messageHandler = new KafkaProducerMessageHandler<Integer, String>(kafkaTemplate);
        messageHandler.setTopicExpression(new LiteralExpression("request_topic"));
        Function<Message<?>, Long> partitionIdFn = (m) -> {
            StepExecutionRequest executionRequest = (StepExecutionRequest) m.getPayload();
            return executionRequest.getStepExecutionId() % 3;
        };
        messageHandler.setPartitionIdExpression(new FunctionExpression<>(partitionIdFn));
                
		return IntegrationFlows.from(requestForWorkers())
				.handle(messageHandler).route("request_topic")
				.get();
	}

	@Bean
	public DirectChannel repliesFromWorkers() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow inboundFlow(ConsumerFactory<Integer, String> consumerFactory) {
		return IntegrationFlows
				.from(Kafka.inboundChannelAdapter(consumerFactory, new ConsumerProperties("reply_topic")))
				.channel(repliesFromWorkers()).get();
	}

	@Bean
	public Job managerJob(Step managerStep) {
		return jobBuilderFactory.get("managerJob").start(managerStep).build();
	}

	@Bean
	public Step managerStep(ColumnRangePartitioner columnRangePartitioner,
			RemotePartitioningManagerStepBuilderFactory remotePartitioningManagerStepBuilderFactory) {
		return remotePartitioningManagerStepBuilderFactory.get("managerStep")
				.partitioner("worker_step", columnRangePartitioner).gridSize(3).outputChannel(requestForWorkers())
				.inputChannel(repliesFromWorkers()).jobExplorer(jobExplorer).build();
	}

}
