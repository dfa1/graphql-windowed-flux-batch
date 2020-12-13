package org.example;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.DataFetcher;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;
import org.dataloader.DataLoaderRegistry;
import org.example.dto.Person;

import java.util.List;
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
		DataLoader<Integer, String> integerPersonDataLoader = DataLoader.newDataLoader(
			keys -> enrichmentService.getEnrichmentValuesInBulk(keys), dataLoaderOptions);

		dataLoaderRegistry.register(ENRICHMENT_DATA_LOADER, integerPersonDataLoader);

		var runtimeWiring = newRuntimeWiring()
			.type("Query", builder ->
				builder.dataFetcher("list", listDataFetcher))
			.type("Person", builder -> builder.dataFetcher("enrichedString", environment -> {
				Person person = environment.getSource();
				return environment.getDataLoader(ENRICHMENT_DATA_LOADER).load(person.getId());
			}))
			.build();

		var graphQlSchema = new SchemaGenerator().makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);
		return GraphQL
			.newGraphQL(graphQlSchema)
			.build();
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
