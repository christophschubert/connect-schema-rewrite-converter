package net.christophschubert.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ConnectClient {

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
        final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response);
    }
}
