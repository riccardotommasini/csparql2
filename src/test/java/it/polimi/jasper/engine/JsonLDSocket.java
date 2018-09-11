package it.polimi.jasper.engine;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RIOT;
import org.apache.jena.sparql.util.Context;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

import com.github.jsonldjava.utils.JsonUtils;

import it.polimi.jasper.streams.RegisteredEPLStream;


@WebSocket(maxTextMessageSize = 1024 * 1024)
public class JsonLDSocket {
	
    @SuppressWarnings("unused")
    private Session session;
    private Context ctx;
    
    private RegisteredEPLStream s;

    public JsonLDSocket(RegisteredEPLStream s, String resourceContext) {
		this.s = s;
		
		ctx = new Context();
	    Object jsonldContextAsObject;
		try {
			jsonldContextAsObject = JsonUtils.fromInputStream(new FileInputStream(JsonLDSocket.class.getResource(resourceContext).getPath()));
			ctx.set(RIOT.JSONLD_CONTEXT, jsonldContextAsObject);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	}

	@OnWebSocketClose
    public void onClose(int statusCode, String reason)
    {
        System.out.printf("Connection closed: %d - %s%n",statusCode,reason);
        this.session = null;
    }

    @OnWebSocketConnect
    public void onConnect(Session session)
    {
        System.out.printf("Got connect: %s%n",session);
        this.session = session;
    }

    @OnWebSocketMessage
    public void onMessage(String msg)
    {
    	StringBuilder sb = new StringBuilder(msg);
    	sb.deleteCharAt(0);
    	sb.deleteCharAt(sb.length() - 1);
    	System.out.println("Batch received");
    	
    	Dataset ds = DatasetFactory.create();
        try (InputStream in = new ByteArrayInputStream(msg.getBytes(StandardCharsets.UTF_8))) {
            RDFParser.create()
                .source(in)
                .lang(Lang.JSONLD)
                .context(ctx)
                .parse(ds.asDatasetGraph());
        } catch (IOException e) {
			e.printStackTrace();
		}
        
        if (s != null)
            this.s.put(ds.getDefaultModel().getGraph(), Instant.now().toEpochMilli());
       
    }
    
}
