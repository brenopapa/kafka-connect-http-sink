package asaintsever.httpsinkconnector.http;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import asaintsever.httpsinkconnector.event.formatter.IEventFormatter;
import asaintsever.httpsinkconnector.http.authentication.AuthException;
import asaintsever.httpsinkconnector.http.authentication.IAuthenticationProvider;


public class HttpEndpoint {
    
    private static final Logger log = LoggerFactory.getLogger(HttpEndpoint.class);

    private String endpoint;
    private final long connectTimeout;
    private final long readTimeout;
    private final int batchSize;
    private final IAuthenticationProvider authenticationProvider;
    private final String contentType;
    private final IEventFormatter eventFormatter;
    private final List<Integer> validStatusCodes;
    private final String carolConnectorId;
    private final String carolAuthorization;
    
    private Map<String,List<SinkRecord>> batches = new HashMap<String,List<SinkRecord>>();
//	List<SinkRecord> batch = new ArrayList<>();
       
    public HttpEndpoint(
            String endpoint,
            long connectTimeout, long readTimeout,
            int batchSize,
            IAuthenticationProvider authenticationProvider,
            String contentType,
            IEventFormatter eventFormatter,
            List<Integer> validStatusCodes,
            String carolConnectorId,
            String carolAuthorization) {
        this.endpoint = endpoint;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.batchSize = batchSize;
        this.authenticationProvider = authenticationProvider;
        this.contentType = contentType;
        this.eventFormatter = eventFormatter;
        this.validStatusCodes = validStatusCodes;
        this.carolConnectorId = carolConnectorId;
        this.carolAuthorization = carolAuthorization;
    }
    
    
    public void write(Collection<SinkRecord> records) throws IOException, InterruptedException {
        for (SinkRecord record : records) {
        	
        	if (this.batches.containsKey(record.topic().toString())) {
        		List<SinkRecord> new_batch = this.batches.get(record.topic().toString());
        		new_batch.add(record);
        		this.batches.put(record.topic().toString(), new_batch);
        	} else {
        		List<SinkRecord> batch = new ArrayList<>();
        		batch.add(record);
        		this.batches.put(record.topic().toString(), batch);
        	}
        	;
//            this.batch.add(record);
            // We got a complete batch: send it
            if (this.batches.get(record.topic().toString()).size() >= this.batchSize) {
            	this.sendBatch(this.batches.get(record.topic().toString()));
            } else {
                log.info("Batch of {} has only {} events, not sending yet!", record.topic().toString(), this.batches.get(record.topic().toString()).size());
            }
        }
        
        // Send remaining records in one last batch (the records left if not enough to constitute a full batch)
//        this.sendBatch();
    }
    
    private void sendBatch(List<SinkRecord> batch) throws IOException, InterruptedException {
    	
        // Skip if batch list is empty
        if (batch.isEmpty())
            return;
        
        log.info("Sending batch of {} events to {}", batch.size(), batch.get(0).topic().toString().replace(".", "").toLowerCase());

        HttpResponse<String> resp;
        
        try {
            resp = this.invoke(this.eventFormatter.formatEventBatch(batch), batch.get(0).topic().toString().replace(".", "").toLowerCase());
        } catch(AuthException authEx) {
            throw new HttpResponseException(this.endpoint, "Authentication error: " + authEx.getMessage());
        } finally {
            // Flush batch list
            batch.clear();
        }
        
        // Check status code
        if (resp != null) {
            for(int statusCode : this.validStatusCodes) {
                if (statusCode == resp.statusCode()) {
                    log.info("Response from HTTP endpoint {}: {} (status code: {})", this.endpoint, resp.body(), resp.statusCode());
                    return;
                }
            }
        }
        
        throw new HttpResponseException(this.endpoint, "Invalid response from HTTP endpoint", resp);
    }
    
    private HttpResponse<String> invoke(byte[] data, String CarolStaging) throws AuthException, IOException, InterruptedException {       
        HttpClient httpCli = HttpClient.newBuilder()
                //.version(Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(this.connectTimeout))
                .build();
        
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                .uri(URI.create(this.endpoint.toString() + "/api/v3/staging/intake/" + CarolStaging + "?connectorId=" + this.carolConnectorId.toString()))
                .timeout(Duration.ofMillis(this.readTimeout))
                .header("Content-Type", this.contentType)
                .header("X-Auth-Key", this.carolAuthorization)
                .header("X-Auth-ConnectorId", this.carolConnectorId)
                .POST(BodyPublishers.ofString(new String(data)))
                ;
//        // Add HTTP authentication header(s)
//        if (this.authenticationProvider.addAuthentication() != null)
//            reqBuilder.headers(this.authenticationProvider.addAuthentication());
       
//        HttpRequest.Builder StagingreqBuilder = HttpRequest.newBuilder()
//                .uri(URI.create("https://datascience.carol.ai/api/v3/staging/tables/autotest/schema?connectorId=e568dea9670746d0a6fec8cd2e3e8027"))
//                .timeout(Duration.ofMillis(this.readTimeout))
//                .header("Content-Type", this.contentType)
//                .POST(BodyPublishers.ofString(new String(
//                		"{\"mdmFlexible\": \"false\", \"mdmExportData\": \"false\",\"mdmStagingMapping\":{\"properties\":{\"cod-emitente\":{\"type\":\"integer\"},\"cgc\":{\"type\":\"string\"}, \"nome-emit\":{\"type\":\"string\"}}},\"mdmCrosswalkTemplate\":{\"mdmCrossreference\":{\"emitente\":[\"cod-emitente\"]}}}"             		
//                		)))
//                ;
//        
//        // Add HTTP authentication header(s)
//        if (this.authenticationProvider.addAuthentication() != null)
//        	StagingreqBuilder.headers(this.authenticationProvider.addAuthentication());
//            reqBuilder.headers(this.authenticationProvider.addAuthentication());
//        
//        httpCli.send(StagingreqBuilder.build(), BodyHandlers.ofString());
        return httpCli.send(reqBuilder.build(), BodyHandlers.ofString());
    }
    
}
