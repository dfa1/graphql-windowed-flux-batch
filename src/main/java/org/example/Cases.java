package org.example;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.schema.DataFetcher;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import org.dataloader.BatchLoaderEnvironment;
import org.dataloader.BatchLoaderWithContext;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;
import org.dataloader.DataLoaderRegistry;
import org.example.dto.Person;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

public class Cases {

	public static final String ENRICHMENT_DATA_LOADER = "enrichmentDataLoader";
	private static final DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry();

	private static GraphQL getGraphQl(EnrichmentService enrichmentService) {
		var typeDefinitionRegistry =
			new SchemaParser()
				.parse(Cases.class.getResourceAsStream("/schema.graphqls"));

		DataFetcher<CompletableFuture<List<Person>>> listDataFetcher =
			dataFetchingEnvironment -> {
				// replacing Source.getPersonList();
				List<Person> input = IntStream.range(1, 100).boxed().map(Person::new).collect(Collectors.toList());
				List<Object> context = Arrays.asList(input.stream().toArray());
				DataLoader<Integer, Person> dataLoader = dataFetchingEnvironment.getDataLoaderRegistry().getDataLoader(ENRICHMENT_DATA_LOADER);
				List<Integer> ids = input.stream().map(Person::getId).collect(Collectors.toList());
				CompletableFuture<List<Person>> listCompletableFuture = dataLoader.loadMany(ids, context);
				return listCompletableFuture;
			};

		DataFetcher<Publisher<Person>> fluxDataPublisher =
			dataFetchingEnvironment ->
				Source.getPersonFlux();

		DataFetcher<Publisher<List<Person>>> windowedFluxDataPublisher =
			dataFetchingEnvironment ->
				Source.getPersonFluxWindowed();

		DataLoaderOptions dataLoaderOptions = DataLoaderOptions.newOptions().setBatchingEnabled(true).setMaxBatchSize(10);
		DataLoader<Integer, Person> integerPersonDataLoader = DataLoader.newDataLoader(new EnrichmentServiceBatched(enrichmentService), dataLoaderOptions);

		dataLoaderRegistry.register(ENRICHMENT_DATA_LOADER, integerPersonDataLoader);

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
	}

	private static class EnrichmentServiceBatched implements BatchLoaderWithContext<Integer, Person> {

		private final EnrichmentService enrichmentService;

		private EnrichmentServiceBatched(EnrichmentService enrichmentService) {
			this.enrichmentService = enrichmentService;
		}

		@Override
		public CompletionStage<List<Person>> load(List<Integer> keys, BatchLoaderEnvironment environment) {
			System.out.println("starting");
			CompletableFuture<List<String>> enrichmentValuesInBulk = enrichmentService.getEnrichmentValuesInBulk(keys);
			return enrichmentValuesInBulk.thenApply(li -> {
				System.out.println("got response");
				List<Object> keyContextsList = environment.getKeyContextsList();
				List<Person> result = new ArrayList<>(li.size());
				for (int i = 0; i < li.size(); i++) {
					Person person = (Person) keyContextsList.get(i);
					String enrichedString = li.get(i);
					person.setEnrichedString(enrichedString);
					result.add(person);
				}
				return result;
			});
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
