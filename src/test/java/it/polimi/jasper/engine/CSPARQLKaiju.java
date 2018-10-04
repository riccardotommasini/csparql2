package it.polimi.jasper.engine;

import it.polimi.jasper.spe.operators.r2s.formatter.ResponseFormatterFactory;
import it.polimi.jasper.streams.RegisteredEPLStream;
import it.polimi.yasper.core.engine.EngineConfiguration;
import it.polimi.yasper.core.spe.operators.r2r.ContinuousQuery;
import it.polimi.yasper.core.spe.operators.r2r.QueryConfiguration;
import it.polimi.yasper.core.spe.operators.r2r.execution.ContinuousQueryExecution;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

/**
 * Created by Riccardo on 03/08/16.
 */
public class CSPARQLKaiju {

    static Jasper sr;

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {

        URL resource = CSPARQLKaiju.class.getResource("/csparqlKaiju.properties");
        QueryConfiguration config = new QueryConfiguration(resource.getPath());
        EngineConfiguration ec = EngineConfiguration.loadConfig("/csparqlKaiju.properties");

        sr = new Jasper(0, ec);

        GraphStreamKaiju writer = new GraphStreamKaiju("ws://localhost:4567/streams/jsonTraces");

        RegisteredEPLStream register = sr.register(writer);

        writer.setWritable(register);

        ContinuousQueryExecution cqe = sr.register(getQuery("Kaiju_exp3_1",".rspql"), config);
        cqe.add(ResponseFormatterFactory.getSelectResponseSysOutFormatter("TABLE", true));//or "CSV" or "JSON" or "JSON-LD"
        ContinuousQueryExecution cqe2 = sr.register(getQuery("42",".rspql"), config);
        cqe2.add(ResponseFormatterFactory.getSelectResponseSysOutFormatter("TABLE", true));//or "CSV" or "JSON" or "JSON-LD"
        
        ArrayList<ContinuousQuery> queries = new ArrayList<>();
        ContinuousQuery q = cqe.getContinuousQuery();
        queries.add(q);
        ContinuousQuery q2 = cqe2.getContinuousQuery();
        queries.add(q2);
        
        for (ContinuousQuery query : queries) {
	        System.out.println(query.toString());
        }

        System.out.println("<<------>>");

        //In real application we do not have to start the stream.
        (new Thread(writer)).start();

    }

    @SuppressWarnings("deprecation")
    public static String getQuery(String nameQuery, String suffix) throws IOException {
        URL resource = ColorsCSPARQLExample.class.getResource("/q" + nameQuery + suffix);
        System.out.println(resource.getPath());
        File file = new File(resource.getPath());
        return FileUtils.readFileToString(file);
    }

}
