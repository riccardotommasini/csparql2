package it.polimi.jasper.engine;

import it.polimi.jasper.engine.rsp.Jasper;
import it.polimi.jasper.engine.rsp.querying.syntax.GraphStream;
import it.polimi.jasper.engine.rsp.streams.RegisteredEPLStream;
import it.polimi.yasper.core.quering.querying.ContinuousQuery;
import it.polimi.yasper.core.utils.EngineConfiguration;
import it.polimi.yasper.core.utils.QueryConfiguration;
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

        GraphStream writer = new GraphStream("Writer", "http://streamreasoning.org/jasper/streams/stream2", 5);

        RegisteredEPLStream register = sr.register(writer);

        writer.setWritable(register);

        ContinuousQuery q2 = sr.parseQuery(getQuery(".rspql"));

        sr.register(q2, config);

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
