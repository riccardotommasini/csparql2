package it.polimi.jasper.engine.geldt;

import it.polimi.jasper.engine.Jasper;
import it.polimi.jasper.engine.execution.formatter.ResponseFormatterFactory;
import it.polimi.jasper.streams.EPLStream;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.jena.graph.Graph;

import java.io.IOException;
import java.net.URL;

/**
 * Created by Riccardo on 03/08/16.
 */
public class GELDTArticleExample extends GELDTExample {


    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {

        URL resource = GELDTArticleExample.class.getResource("/geldt/csparqlGELDT.properties");
        SDSConfiguration config = new SDSConfiguration(resource.getPath());
        EngineConfiguration ec = EngineConfiguration.loadConfig("/geldt/csparqlGELDT.properties");

        sr = new Jasper(0, ec);

        type = "article";
        GELDTGraphStream dt = new GELDTGraphStream(3, "Donald Trump", type);

        System.out.println(dt);

        EPLStream<Graph> dtr = sr.register(dt);

        dt.setWritable(dtr);

        ContinuousQueryExecution cqe = sr.register(getQuery("OneStream", ".rspql", type), config);

        new Thread(dt).start();

        if (cqe.getContinuousQuery().isConstructType()) {
            cqe.add(ResponseFormatterFactory.getConstructResponseSysOutFormatter("JSON-LD", true));
        } else if (cqe.getContinuousQuery().isSelectType()) {
            cqe.add(ResponseFormatterFactory.getSelectResponseSysOutFormatter("CSV", true));
        }
    }

}
