package org.example;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Fake enrichment service
 */
public class EnrichmentService {
    private int numberOfTimesCalled;

    /**
     * To be used for assertions, gets the number of times the {@link #getEnrichmentValuesInBulk(List)} method was called
     * @return Number of calls to the enrichment method
     */
    public int getNumberOfTimesCalled() {
        return numberOfTimesCalled;
    }

    /**
     * Stub of an enrichment call
     * @param ids ids of people for which we want the enrichment string
     * @return a list of enrichment results
     */
    public CompletableFuture<List<String>> getEnrichmentValuesInBulk(List<Integer> ids) {
        this.numberOfTimesCalled++;
        System.out.printf("bulk %s%n", ids);
        return CompletableFuture.supplyAsync(() ->
                ids
                        .stream()
                        .map(id ->
                                String.format("EnrichmentFor%s", id))
                        .collect(Collectors.toList()));
    }

}
