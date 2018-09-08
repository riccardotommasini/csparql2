package it.polimi.jasper.engine;

import it.polimi.jasper.streams.RegisteredEPLStream;
import it.polimi.jasper.streams.schema.GraphStreamSchema;
import it.polimi.yasper.core.stream.rdf.RDFStream;
import it.polimi.yasper.core.stream.schema.StreamSchema;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;

public class GraphStreamKaiju extends RDFStream implements Runnable {

    private StreamSchema schema = new GraphStreamSchema();

    @Override
    public StreamSchema getSchema() {
        return schema;
    }

    private RegisteredEPLStream s;

    public GraphStreamKaiju(String stream_uri) {
        super(stream_uri);
    }

    public void setWritable(RegisteredEPLStream e) {
        this.s = e;
    }

    public void run() {
    	
    	String destUri = "ws://localhost:4567/jsonTraces";
        WebSocketClient client = new WebSocketClient();
        SimpleClient socket = new SimpleClient(s);
        try
        {
            client.start();

            URI uri = new URI(destUri);
            ClientUpgradeRequest request = new ClientUpgradeRequest();
            client.connect(socket,uri,request);
            System.out.printf("Connecting to : %s%n",uri);
            
            while(true) {}

        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }
        finally
        {
            try
            {
            	client.stop();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}
