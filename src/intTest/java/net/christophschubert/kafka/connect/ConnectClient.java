package net.christophschubert.kafka.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;

public class ConnectClient {

    final static Logger logger = LoggerFactory.getLogger(ConnectClient.class);

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();

    public ConnectClient(String baseURL) {
        this.baseUrl = baseURL;
        this.httpClient = HttpClient.newBuilder().build();
    }


    public void startConnector(ConnectorConfig config) throws IOException, InterruptedException {
        final var request = HttpRequest.newBuilder(URI.create(baseUrl + "/connectors"))
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(config)))
                .header("Content-Type", "application/json")
                .build();
        logger.info("submitting config: " + mapper.writeValueAsString(config));
        final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        logger.info(response.toString());
    }

    public List<String> getConnectors() {
        return Collections.emptyList(); //TODO: implement
    }
}
