package org.example;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.execution.ValueUnboxer;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import org.dataloader.BatchLoaderEnvironment;
import org.dataloader.BatchLoaderWithContext;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;
import org.dataloader.DataLoaderRegistry;
import org.example.dto.Person;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

public class Cases {

	public static final String ENRICHMENT_DATA_LOADER = "enrichmentDataLoader";
	private static final DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry();

	private static GraphQL getGraphQl(EnrichmentService enrichmentService) {
		var typeDefinitionRegistry =
			new SchemaParser()
				.parse(Cases.class.getResourceAsStream("/schema.graphqls"));

		DataFetcher<Stream<Person>> listDataFetcher =
			dataFetchingEnvironment -> {
				// replacing Source.getPersonList();
				// avoiding allocating a big list upfront...
				return IntStream.range(1, 100).boxed().map(Person::new);
			};

		DataLoaderOptions dataLoaderOptions = DataLoaderOptions.newOptions().setBatchingEnabled(true).setMaxBatchSize(10);
		DataLoader<Integer, String> integerPersonDataLoader = DataLoader.newDataLoader(new EnrichmentServiceBatched(enrichmentService), dataLoaderOptions);

		dataLoaderRegistry.register(ENRICHMENT_DATA_LOADER, integerPersonDataLoader);

		var runtimeWiring = newRuntimeWiring()
			.type("Query", builder ->
				builder.dataFetcher("list", listDataFetcher))
			.type("Person", builder -> builder.dataFetcher("enrichedString", new DataFetcher() {
				@Override
				public Object get(DataFetchingEnvironment environment) throws Exception {
					System.out.println(environment.getField());
					return null;
				}
			}))
			.build();

		var graphQlSchema = new SchemaGenerator().makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);
		return GraphQL
			.newGraphQL(graphQlSchema)
			.valueUnboxer(new ValueUnboxer() {
				@Override
				public Object unbox(Object object) {
					if (object instanceof CompletableFuture) {
						return ((CompletableFuture<Object>) object).join();
					}
					return object;
				}
			})
			.subscriptionExecutionStrategy(new SubscriptionExecutionStrategy())
			.build();
	}

	private static class EnrichmentServiceBatched implements BatchLoaderWithContext<Integer, String> {

		private final EnrichmentService enrichmentService;

		private EnrichmentServiceBatched(EnrichmentService enrichmentService) {
			this.enrichmentService = enrichmentService;
		}

		@Override
		public CompletionStage<List<String>> load(List<Integer> keys, BatchLoaderEnvironment environment) {
			return enrichmentService.getEnrichmentValuesInBulk(keys);
		}
	}

	public static ExecutionResult queryingForList(EnrichmentService enrichmentService) {
		return getGraphQl(enrichmentService).execute(ExecutionInput.newExecutionInput()
			.query("query list {\n" +
				"    list {\n" +
				"        id\n" +
				"        enrichedString\n" +
				"    }\n" +
				"}")
			.dataLoaderRegistry(dataLoaderRegistry)
			.build());
	}
}
