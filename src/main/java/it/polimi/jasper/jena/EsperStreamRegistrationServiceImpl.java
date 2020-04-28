package it.polimi.jasper.jena;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;
import it.polimi.jasper.engine.esper.EsperStreamRegistrationService;
import it.polimi.jasper.streams.EPLStream;
import it.polimi.yasper.core.exceptions.StreamRegistrationException;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import it.polimi.yasper.core.stream.web.WebStream;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

@Log4j
public class EsperStreamRegistrationServiceImpl extends EsperStreamRegistrationService<Graph> {

    public EsperStreamRegistrationServiceImpl(EPAdministrator cepAdm) {
        super(cepAdm);
    }

    public DataStreamImpl<Graph> register(WebStream s) {
        String uri = s.getURI();
        log.info("Registering Stream [" + uri + "]");
        if (!registeredStreams.containsKey(uri)) {
            EPStatement epl = createStream(toEPLSchema(s), uri);
            log.info(epl.getText());
            EPLStream<Graph> value = new EPLGraphRDFStream(s.getURI(), s, epl);
            registeredStreams.put(uri, value);
            return value;
        } else
            throw new StreamRegistrationException("Stream [" + uri + "] already registered");
    }

}
