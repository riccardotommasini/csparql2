package it.polimi.jasper.dbpedia;

import it.polimi.jasper.streams.RegisteredEPLStream;
import it.polimi.jasper.streams.schema.GraphStreamSchema;
import it.polimi.yasper.core.stream.rdf.RDFStream;
import it.polimi.yasper.core.stream.schema.StreamSchema;
import org.apache.jena.rdf.model.*;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;

/**
 * Created by Riccardo on 13/08/16.
 */
@WebSocket
public class DBPediaStream extends RDFStream implements Runnable {

    private final String ws;
    private StreamSchema schema = new GraphStreamSchema();
    private boolean isDone = false;
    private long counter = 0;

    @Override
    public StreamSchema getSchema() {
        return schema;
    }

    private RegisteredEPLStream s;


    public DBPediaStream(String ws, String stream_uri) {
        super(stream_uri);
        this.ws = ws;
    }

    public void setWritable(RegisteredEPLStream e) {
        this.s = e;
    }

    public void start() {
        System.out.println(ws);
        try {
            Thread thread1 = new Thread(this);
            thread1.start();
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @OnWebSocketConnect
    public void onConnect(Session user) throws Exception {
        System.out.println("Connecting on:\t" + System.nanoTime());
    }

    @OnWebSocketClose
    public void onClose(Session user, int statusCode, String reason) {
        System.out.println("Closing on:" + System.nanoTime() + "\tprocessed:\t" + counter);
        Model md = ModelFactory.createDefaultModel();
        isDone = true;//this is used to know when we can close the program
        //after closing, we send some empty graphs
        for (int i = 0; i < 60000; i += 1000) {
            this.s.put(md.getGraph(), System.nanoTime() / 1000000);
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    public boolean isDone() {
        return isDone;
    }

    @OnWebSocketMessage
    public void onMessage(Session user, String message) {
        if (message.startsWith("<")) {
            try {
                counter++;
                String[] triples = message.split(" ");
                for (int i = 0; i < 3; i++) {
                    triples[i] = triples[i].substring(1, triples[i].length() - 1);
                }
                Model m = ModelFactory.createDefaultModel();

                Resource subject = ResourceFactory.createResource(triples[0]);
                Property property = ResourceFactory.createProperty(triples[1]);
                Resource object = ResourceFactory.createResource(triples[2]);
                m.add(m.createStatement(subject, property, object));
                //send the received message to the engine
                this.s.put(m.getGraph(), System.nanoTime() / 1000000);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        WebSocketClient client = new WebSocketClient();
        try {
            client.start();
            client.connect(this, new URI(ws));
            System.out.printf("Connecting to : %s%n", ws);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
