package com.spring.etl.worker.application.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class AddressStepExecutionListener implements StepExecutionListener {

	@Override
	public void beforeStep(StepExecution stepExecution) {
		System.out.println("before step");
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		System.out.println("after step");
		return ExitStatus.COMPLETED;
	}

}
