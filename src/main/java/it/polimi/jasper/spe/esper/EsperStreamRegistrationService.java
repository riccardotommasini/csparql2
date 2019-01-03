package it.polimi.jasper.spe.esper;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.soda.CreateSchemaClause;
import com.espertech.esper.common.client.soda.SchemaColumnDesc;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPDeploymentService;
import com.espertech.esper.runtime.client.EPListenable;
import it.polimi.jasper.spe.EncodingUtils;
import it.polimi.jasper.streams.RegisteredEPLStream;
import it.polimi.yasper.core.engine.exceptions.StreamRegistrationException;
import it.polimi.yasper.core.engine.exceptions.UnregisteredStreamExeception;
import it.polimi.yasper.core.stream.RegisteredStream;
import it.polimi.yasper.core.stream.Stream;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

import java.io.StringWriter;
import java.util.*;

@Log4j
public class EsperStreamRegistrationService {

    private final EPDeploymentService cepAdm;
    private final Configuration config;
    @Getter
    private Map<String, RegisteredStream<Graph>> registeredStreams;

    public EsperStreamRegistrationService(EPDeploymentService cepAdm) {
        this.cepAdm = cepAdm;
        this.config = RuntimeManager.getRuntimeConfiguration();
        this.registeredStreams = new HashMap<>();
    }

    public RegisteredEPLStream register(Stream s) {
        try {
            String uri = s.getURI();
            log.info("Registering Stream [" + uri + "]");
            if (!registeredStreams.containsKey(uri)) {
                EPDeployment epl = createStream(toEPLSchema(s), uri);
                //TODO careful on the deployment id
                RegisteredEPLStream value = new RegisteredEPLStream(s.getURI(), s, epl);
                registeredStreams.put(uri, value);
                return value;
            } else
                throw new StreamRegistrationException("Stream [" + uri + "] already registered");
        } catch (EPCompileException e) {
            throw new StreamRegistrationException("Compilation Error [" + s.getURI() + "]");
        } catch (EPDeployException e) {
            throw new StreamRegistrationException("Deployment Error [" + s.getURI() + "]");
        }
    }

    public void unregister(RegisteredEPLStream s) {
        log.info("Unregistering Stream [" + s + "]");
        if (!isRegistered(s.getURI())) {
            throw new UnregisteredStreamExeception("Stream [" + s.getURI() + "] not registered");
        }
        EPDeployment deployment = cepAdm.getDeployment(s.getURI());
        Arrays.stream(deployment.getStatements()).forEach(EPListenable::removeAllListeners);
        registeredStreams.remove(EncodingUtils.encode(s.getStream().getURI()));
    }

    protected EPDeployment createStream(String epl, String uri) throws EPCompileException, EPDeployException {
        String s = EncodingUtils.encode(uri);
        String replace = epl.replace(uri, s);
        log.info("EPL Schema Statement [ " + replace + "]");
        return cepAdm.deploy(EPCompilerProvider.getCompiler().compile(replace, new CompilerArguments(config)));
    }

    private String toEPLSchema(Stream s) {
        CreateSchemaClause schema = new CreateSchemaClause();
        schema.setSchemaName(EncodingUtils.encode(s.getURI()));
        schema.setInherits(new HashSet<>(Arrays.asList("TStream")));
        List<SchemaColumnDesc> columns = new ArrayList<>();
        schema.setColumns(columns);
        StringWriter writer = new StringWriter();
        schema.toEPL(writer);
        return writer.toString();
    }

    public boolean isRegistered(Stream s) {
        return isRegistered(s.getURI());
    }

    public boolean isRegistered(String s) {
        return registeredStreams.containsKey(s);
    }

}
