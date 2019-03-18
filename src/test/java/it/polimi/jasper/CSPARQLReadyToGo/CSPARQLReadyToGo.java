package it.polimi.jasper.CSPARQLReadyToGo;

import it.polimi.jasper.CSPARQLReadyToGo.streams.LBSMARDFStreamTestGenerator;
import it.polimi.jasper.engine.GraphStream;
import it.polimi.jasper.engine.Jasper;
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
public class CSPARQLReadyToGo {

    static Jasper sr;

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {

        // examples name
        final int WHO_LIKES_WHAT = 0;
        final int HOW_MANY_USERS_LIKE_THE_SAME_OBJ = 1;
        final int MULTI_STREAM = 2;

        // put here the example you want to run
        int key = MULTI_STREAM;

        URL resource = CSPARQLReadyToGo.class.getResource("/csparql.properties");
        QueryConfiguration config = new QueryConfiguration(resource.getPath());
        EngineConfiguration ec = EngineConfiguration.loadConfig("/csparql.properties");

        ContinuousQuery q;
        ContinuousQueryExecution cqe;
        LBSMARDFStreamTestGenerator writer;
        RegisteredEPLStream register;

        sr = new Jasper(0, ec);

        switch (key) {
            case WHO_LIKES_WHAT:
                
                System.out.println("WHO_LIKES_WHAT example");

                writer = new LBSMARDFStreamTestGenerator("Writer", "http://streamreasoning.org/jasper/streams/stream2", 5);
                register = sr.register(writer);
                writer.setWritable(register);

                cqe = sr.register(getQuery("rtgp-q1",".rspql"), config);
                q = cqe.getContinuousQuery();
                cqe.add(new GenericResponseSysOutFormatter("TABLE", true));

                System.out.println(q.toString());
                System.out.println("<<------>>");

                //In real application we do not have to start the stream.
                (new Thread(writer)).start();

                break;
            case HOW_MANY_USERS_LIKE_THE_SAME_OBJ:

                writer = new LBSMARDFStreamTestGenerator("Writer", "http://streamreasoning.org/jasper/streams/stream2", 5);
                register = sr.register(writer);
                writer.setWritable(register);

                cqe = sr.register(getQuery("rtgp-q2",".rspql"), config);
                q = cqe.getContinuousQuery();
                cqe.add(new GenericResponseSysOutFormatter("TABLE", true));

                System.out.println(q.toString());
                System.out.println("<<------>>");

                //In real application we do not have to start the stream.
                (new Thread(writer)).start();
                break;

            case MULTI_STREAM:

                writer = new LBSMARDFStreamTestGenerator("Writer", "http://streamreasoning.org/jasper/streams/stream2", 5);
                register = sr.register(writer);
                writer.setWritable(register);

                LBSMARDFStreamTestGenerator writer2 = new LBSMARDFStreamTestGenerator("Writer", "http://streamreasoning.org/jasper/streams/stream3", 5);
                RegisteredEPLStream register2 = sr.register(writer2);
                writer2.setWritable(register2);

                cqe = sr.register(getQuery("rtgp-q3",".rspql"), config);
                q = cqe.getContinuousQuery();
                cqe.add(new GenericResponseSysOutFormatter("TABLE", true));

                System.out.println(q.toString());
                System.out.println("<<------>>");

                //In real application we do not have to start the stream.
                (new Thread(writer)).start();
                break;
            default:
                System.exit(0);
                break;
        }
    }

    public static String getQuery(String queryName, String suffix) throws IOException {
        URL resource = CSPARQLReadyToGo.class.getResource("/" + queryName + suffix);
        System.out.println(resource.getPath());
        File file = new File(resource.getPath());
        return FileUtils.readFileToString(file);
    }

}
