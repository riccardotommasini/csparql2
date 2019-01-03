package it.polimi.jasper.dbpedia;

import it.polimi.jasper.engine.Jasper;
import it.polimi.jasper.spe.operators.r2s.results.SelectResponse;
import it.polimi.jasper.streams.RegisteredEPLStream;
import it.polimi.yasper.core.engine.EngineConfiguration;
import it.polimi.yasper.core.spe.operators.r2r.ContinuousQuery;
import it.polimi.yasper.core.spe.operators.r2r.QueryConfiguration;
import it.polimi.yasper.core.spe.operators.r2r.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.ResultSetRewindable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Observable;

/**
 * Created by Riccardo on 03/08/16.
 */
public class DBPeddiaTest {

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {
        // URL resource =
        // CSPARQLExample.class.getResource("/csparql.properties");
        if (args.length < 4) {
            System.out.println("Usage: [window size]  [config file path] [query path] [ws url]");
            System.exit(0);
        }

        int windowSize = Integer.parseInt(args[0]); //size of the window, injected in the query

        String configPath = args[1]; //file path to the property file
        String queryPath = args[2];  //file path to the query
        String url = args[3];         //websocket url
        // load the query, passed as a file
        String query = getQuery(queryPath);

        // adapt the query to include the window parameters (they change during
        // the experiment)
        query = String.format(query, windowSize, windowSize);

        System.out.println(query);
        // load the config file from file
        URL resource = new File(configPath).toURL();

        QueryConfiguration config = new QueryConfiguration(resource.getPath());
        EngineConfiguration ec = new EngineConfiguration(resource.getPath());

        Jasper sr = new Jasper(0, ec);
        //create a graph streamer that is a websocket client
        DBPediaStream writer = new DBPediaStream(url, "http://streamreasoning.org/iminds/massif/dbpediastream");

        RegisteredEPLStream register = sr.register(writer);

        writer.setWritable(register);

        ContinuousQueryExecution cqe = sr.register(query, config);


        boolean verbose = true;

        cqe.add(new QueryResultFormatter("TABLE", verbose) {
            private FileOutputStream fop;
            long last_result = -1L;
            private DBPediaStream stream;

            @Override
            public void update(Observable o, Object arg) {
                if (writer.isDone()) {
                    System.out.println("Done At:\t" + System.nanoTime());
                    System.exit(0);
                } else if (arg instanceof SelectResponse) {
                    SelectResponse sr = (SelectResponse) arg;
                    if (sr.getCep_timestamp() != last_result) {
                        last_result = sr.getCep_timestamp();
                        ResultSetRewindable resultSetRewindable = ResultSetFactory.copyResults(sr.getResults());
                        if (distinct)
                            ResultSetFormatter.out(System.out, resultSetRewindable);

                        System.out.println(
                                sr.getSolutionSet().size() +
                                        " Triples at sys time " +
                                        "[" + System.currentTimeMillis() + "] " +
                                        "event time [" + last_result + "]");

                        //if the stream is done streaming and no results were found, we close the program

                    }
                }
            }
        });

        ContinuousQuery q2 = cqe.getContinuousQuery();

        System.out.println(q2.toString());

        System.out.println("<<------>>");

        writer.start();
    }

    public static String getQuery(String path) throws IOException {
        File file = new File(path);
        return FileUtils.readFileToString(file);
    }
}
