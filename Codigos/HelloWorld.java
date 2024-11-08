import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchExample {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchExample.class);
    private static final String INDEX_NAME = "meu_indice";

    public static void main(String[] args) {
        try (RestHighLevelClient client = createClient()) {
            createIndex(client, INDEX_NAME);
            indexDocuments(client);
            searchDocuments(client);
            updateDocument(client, "1", "Elasticsearch é realmente incrível!");
            deleteIndex(client, INDEX_NAME);
        } catch (IOException e) {
            logger.error("Erro ao executar operações do Elasticsearch", e);
        }
    }

    private static RestHighLevelClient createClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        return new RestHighLevelClient(builder);
    }

    private static void createIndex(RestHighLevelClient client, String indexName) throws IOException {
        client.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
        logger.info("Índice criado: {}", indexName);
    }

    private static void indexDocuments(RestHighLevelClient client) throws IOException {
        Map<String, Object> document1 = new HashMap<>();
        document1.put("autor", "John Doe");
        document1.put("texto", "Elasticsearch é incrível!");
        document1.put("timestamp", "2024-10-31T16:22:50");

        Map<String, Object> document2 = new HashMap<>();
        document2.put("autor", "Jane Smith");
        document2.put("texto", "Java é ótimo para análise de dados.");
        document2.put("timestamp", "2024-10-31T16:23:50");

        indexDocument(client, INDEX_NAME, "1", document1);
        indexDocument(client, INDEX_NAME, "2", document2);
    }

    private static void indexDocument(RestHighLevelClient client, String indexName, String id,
            Map<String, Object> document) throws IOException {
        IndexRequest request = new IndexRequest(indexName).id(id).source(document, XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        logger.info("Documento indexado: {}", response.getId());
    }

    private static void searchDocuments(RestHighLevelClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        logger.info("Documentos encontrados: {}", searchResponse.getHits().getHits().length);
    }

    private static void updateDocument(RestHighLevelClient client, String id, String newText) throws IOException {
        Map<String, Object> updateFields = new HashMap<>();
        updateFields.put("texto", newText);
        UpdateRequest request = new UpdateRequest(INDEX_NAME, id).doc(updateFields);
        client.update(request, RequestOptions.DEFAULT);
        logger.info("Documento atualizado: {}", id);
    }

    private static void deleteIndex(RestHighLevelClient client, String indexName) throws IOException {
        client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
        logger.info("Índice excluído: {}", indexName);
    }
}


