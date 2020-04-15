package it.polimi.seraph;

import it.polimi.jasper.streams.EPLRDFStream;
import it.polimi.seraph.streans.EPLPGraphStream;
import it.polimi.yasper.core.format.QueryResultFormatter;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;

import java.util.Observable;

public class KaypherExample {

    static Kaypher sr = new Kaypher(0, null);

    public static void main(String[] args) {

        SDSConfiguration config = null;

        //Streams

        PGraphStream writer = null;

        EPLPGraphStream register = sr.register(writer);

        writer.setWritable(register);

        //Register the query

        Seraph searph = new Seraph("MATCH (n:Person) RETURN n.person_name AS name LIMIT 10");


        ContinuousQueryExecution cqe = sr.register(searph, config);

        cqe.add(new QueryResultFormatter("Neo4j", true) {
            @Override
            public void update(Observable o, Object arg) {




            }
        });

        //In real application we do not have to start the stream.
        (new Thread(writer)).start();

    }
}
