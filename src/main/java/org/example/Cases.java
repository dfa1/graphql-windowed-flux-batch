package org.example;

import com.google.common.io.Resources;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.schema.DataFetcher;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;
import org.dataloader.DataLoaderRegistry;
import org.example.dto.Person;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

@SuppressWarnings({"Convert2MethodRef", "UnstableApiUsage"})
public class Cases {
	public static final String ENRICHMENT_DATA_LOADER = "enrichmentDataLoader";
	private static final DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry();

	private static GraphQL getGraphQl(EnrichmentService enrichmentService) {
		try {
			var typeDefinitionRegistry =
				new SchemaParser()
					.parse(Resources.toString(Resources.getResource("schema.graphqls"), StandardCharsets.UTF_8));

			DataFetcher<CompletableFuture<List<Person>>> listDataFetcher =
				dataFetchingEnvironment -> {
					// replacing Source.getPersonList();
					List<Person> personList = IntStream.range(1, 100).boxed().map(Person::new).collect(Collectors.toList());
					DataLoader<Integer, String> dataLoader = dataFetchingEnvironment.getDataLoaderRegistry().getDataLoader(ENRICHMENT_DATA_LOADER);
					List<Integer> collect = personList.stream().map(Person::getId).collect(Collectors.toList());
					return dataLoader.loadMany(collect).thenApply(result -> {
						// this is enriching the original input List<Person>
						for (int i = 0; i < personList.size(); i++) {
							personList.get(i).setEnrichedString(result.get(i));
						}
						return personList;
					});
				};


			DataFetcher<Publisher<Person>> fluxDataPublisher =
				dataFetchingEnvironment ->
					Source.getPersonFlux();

			DataFetcher<Publisher<List<Person>>> windowedFluxDataPublisher =
				dataFetchingEnvironment ->
					Source.getPersonFluxWindowed();

			DataLoaderOptions dataLoaderOptions = DataLoaderOptions.newOptions().setBatchingEnabled(true).setMaxBatchSize(10);
			DataLoader<Integer, String> enrichmentStringBatchLoader =
				DataLoader.newDataLoader(keys ->
					enrichmentService.getEnrichmentValuesInBulk(keys), dataLoaderOptions);

			dataLoaderRegistry.register(ENRICHMENT_DATA_LOADER, enrichmentStringBatchLoader);

			var runtimeWiring = newRuntimeWiring()
				.type("Query", builder ->
					builder.dataFetcher("list", listDataFetcher))
				.type("Subscription", builder ->
					builder.dataFetcher("flux", fluxDataPublisher))
				.type("Subscription", builder ->
					builder.dataFetcher("fluxWindowed", windowedFluxDataPublisher))
				.type("Person", builder ->
					builder.dataFetcher("enrichedString", dataFetchingEnvironment -> {
						Person person = dataFetchingEnvironment.getSource();
						return dataFetchingEnvironment.getDataLoader(ENRICHMENT_DATA_LOADER).load(person.getId());
					}))
				.build();

			var graphQlSchema = new SchemaGenerator().makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);
			return GraphQL
				.newGraphQL(graphQlSchema)
				.subscriptionExecutionStrategy(new SubscriptionExecutionStrategy())
				.build();

		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}


	public static void queryingForList(EnrichmentService enrichmentService) {
		var executionResult = getGraphQl(enrichmentService).execute(ExecutionInput.newExecutionInput()
			.query("query list {\n" +
				"    list {\n" +
				"        id\n" +
				"        enrichedString\n" +
				"    }\n" +
				"}")
			.dataLoaderRegistry(dataLoaderRegistry)
			.build());

		// I don't really care about the result, all that matters is how many times the enrichmentService was called
		// but you can have a look at executionResult.getData()
	}

	public static void subscribingToFlux(EnrichmentService enrichmentService) {
		var executionResult = getGraphQl(enrichmentService).execute(ExecutionInput.newExecutionInput()
			.query("subscription sub {\n" +
				"    flux {\n" +
				"        id\n" +
				"        enrichedString\n" +
				"    }\n" +
				"}")
			.dataLoaderRegistry(dataLoaderRegistry)
			.build());

		Flux.from(executionResult.getData())
			.collectList()
			.block();
		// of course I wouldn't block, but instead return the flux as server-sent events.
		// again, I don't care about the data, only how many times the enrichmentService was called, which I assert in the test
	}

	public static Flux<List<Person>> subscribingToWindowedFlux(EnrichmentService enrichmentService) {
		var executionResult = getGraphQl(enrichmentService).execute(ExecutionInput.newExecutionInput()
			.query("subscription sub {\n" +
				"    fluxWindowed {\n" +
				"        id\n" +
				"        enrichedString\n" +
				"    }\n" +
				"}")
			.dataLoaderRegistry(dataLoaderRegistry)
			.build());

		Publisher<ExecutionResult> executionResults = executionResult.getData();
		var res = Flux.from(executionResults)
			.map(executionRes -> {
				Map<String, List<Person>> singleWindowResults = executionRes.getData();
				List<Person> peopleWindow = singleWindowResults.get("fluxWindowed");
				return peopleWindow;
			});
		return res;
	}
}
