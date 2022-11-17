package com.spring.etl.manager.jacksonutils;

import java.util.Collection;

import org.springframework.batch.core.StepExecution;

import com.fasterxml.jackson.annotation.JsonManagedReference;

public abstract class JobExecutionMixin {
	@JsonManagedReference
	private Collection<StepExecution> stepExecutions;
}
