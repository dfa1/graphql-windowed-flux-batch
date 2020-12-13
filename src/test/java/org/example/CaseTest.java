package org.example;

import graphql.ExecutionResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CaseTest {
	@Test
	public void should_batch_when_queryingForList() {
		var enrichmentService = new EnrichmentService();
		ExecutionResult executionResult = Cases.queryingForList(enrichmentService);

		System.out.println(executionResult.toSpecification());

		// source is 100 persons with batching of 10
		assertEquals(10, enrichmentService.getNumberOfTimesCalled());

	}

}
