package it.polimi.jasper.engine;

import it.polimi.jasper.streams.RegisteredEPLStream;
import it.polimi.yasper.core.stream.rdf.RDFStream;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class GraphStreamKaiju extends RDFStream implements Runnable {
	
	private final static Logger log = LoggerFactory.getLogger(GraphStreamKaiju.class); 
    private RegisteredEPLStream s;

    public GraphStreamKaiju(String stream_uri) {
        super(stream_uri);
    }

    public void setWritable(RegisteredEPLStream e) {
        this.s = e;
    }

    public void run() {
    	
        WebSocketClient client = new WebSocketClient();
        JsonLDSocket socket = new JsonLDSocket(s, "/tracing_ontology_context.json");
        
        try
        {
            client.start();

            URI uri = new URI(stream_uri);
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            client.connect(socket, uri, request);
            System.out.printf("Connecting to : %s%n",uri);
            
            while(true) {}

        }
        catch (Throwable t)
        {
            log.error("Socket error");
        }
        finally
        {
            try
            {
            	client.stop();
            }
            catch (Exception e)
            {
            	log.error("Error closing socket");
            }
        }
    }
}
