package it.polimi.jasper.engine;

import it.polimi.jasper.spe.operators.r2s.formatter.sysout.SelectSysOutDefaultFormatter;
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

        ContinuousQueryExecution cqe = sr.register(getQuery(".rspql"), config);
        cqe.add(new SelectSysOutDefaultFormatter("TABLE", true)); //or "CSV" or "JSON" or "JSON-LD"
        
        ContinuousQuery q2 = cqe.getContinuousQuery();

        System.out.println(q2.toString());

        System.out.println("<<------>>");

        //In real application we do not have to start the stream.
        (new Thread(writer)).start();

    }

    public static String getQuery(String suffix) throws IOException {
        URL resource = CSPARQLKaiju.class.getResource("/q42" + suffix);
        System.out.println(resource.getPath());
        File file = new File(resource.getPath());
        return FileUtils.readFileToString(file);
    }

}
