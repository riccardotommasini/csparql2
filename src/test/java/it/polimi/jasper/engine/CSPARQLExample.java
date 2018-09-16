package it.polimi.jasper.engine;

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
public class CSPARQLExample {

    static Jasper sr;

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {

        URL resource = CSPARQLExample.class.getResource("/csparql.properties");
        QueryConfiguration config = new QueryConfiguration(resource.getPath());
        EngineConfiguration ec = EngineConfiguration.loadConfig("/csparql.properties");

        sr = new Jasper(0, ec);

        GraphStream writer = new GraphStream("Writer", "http://streamreasoning.org/jasper/streams/stream2", 1);

        RegisteredEPLStream register = sr.register(writer);

        writer.setWritable(register);

        ContinuousQueryExecution cqe = sr.register(getQuery(".rspql"), config);

        ContinuousQuery q2 = cqe.getContinuousQuery();

        System.out.println(q2.toString());

        System.out.println("<<------>>");

        //In real application we do not have to start the stream.
        (new Thread(writer)).start();

    }

    public static String getQuery(String suffix) throws IOException {
        URL resource = CSPARQLExample.class.getResource("/q52" + suffix);
        System.out.println(resource.getPath());
        File file = new File(resource.getPath());
        return FileUtils.readFileToString(file);
    }

}
