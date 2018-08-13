package it.polimi.jasper.engine.spe;

import it.polimi.jasper.engine.querying.syntax.QueryFactory;
import it.polimi.jasper.engine.reasoning.EntailmentImpl;
import it.polimi.jasper.engine.reasoning.ReasoningUtils;
import it.polimi.jasper.engine.sds.JasperSDSBuilder;
import it.polimi.yasper.core.enums.EntailmentType;
import it.polimi.yasper.core.exceptions.UnregisteredQueryExeception;
import it.polimi.yasper.core.quering.ContinuousQuery;
import it.polimi.yasper.core.quering.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.quering.formatter.QueryResponseFormatter;
import it.polimi.yasper.core.stream.Stream;
import it.polimi.yasper.core.utils.EngineConfiguration;
import it.polimi.yasper.core.utils.QueryConfiguration;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.riot.system.IRIResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
public class CSPARQLEngine extends EsperRSPEngine {

    @Getter
    private final IRIResolver resolver;

    public CSPARQLEngine(long t0, EngineConfiguration configuration) {
        super(t0, configuration);
        this.resolver = IRIResolver.create(configuration.getBaseURI());

        this.entailments = new HashMap<>();

        //Adding default entailments
        String ent = EntailmentType.RDFS.name();
        this.entailments.put(ent, new EntailmentImpl(ent, Rule.rulesFromURL(ReasoningUtils.RHODF_RULE_SET_RUNTIME), EntailmentType.RDFS));
        ent = EntailmentType.RHODF.name();
        this.entailments.put(ent, new EntailmentImpl(ent, Rule.rulesFromURL(ReasoningUtils.RHODF_RULE_SET_RUNTIME), EntailmentType.RHODF));

    }

    @Override
    public ContinuousQuery parseQuery(String q) {
        log.info("Parsing Query [" + q + "]");
        try {
            return QueryFactory.parse(resolver, q);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ContinuousQueryExecution register(ContinuousQuery q, QueryConfiguration c) {
        Map<String, Stream> registeredStreams = stream_registration_service.getRegisteredStreams();
        JasperSDSBuilder builder =
                new JasperSDSBuilder(registeredStreams, entailments, rsp_config, c, resolver);
        builder.visit(q);
        ContinuousQueryExecution continuousQueryExecution = builder.getContinuousQueryExecution();
        stream_dispatching_service.putAll(builder.getWindowAssigners());
        save(q, continuousQueryExecution, builder.getSDS());
        //register(new QueryStream(this, q.getID(), RDFStreamItem.class));
        return continuousQueryExecution;
    }


    @Override
    public void register(ContinuousQuery q, QueryResponseFormatter f) {
        String qID = q.getID();
        log.info("Registering Observer [" + f.getClass() + "] to Query [" + qID + "]");
        if (!registeredQueries.containsKey(qID))
            throw new UnregisteredQueryExeception(qID);
        else {
            ContinuousQueryExecution ceq = queryExecutions.get(qID);
            ceq.addFormatter(f);
            createQueryObserver(f, qID);
        }
    }

    private void createQueryObserver(QueryResponseFormatter o, String qID) {
        if (queryObservers.containsKey(qID)) {
            List<QueryResponseFormatter> l = queryObservers.get(qID);
            if (l != null) {
                l.add(o);
            } else {
                l = new ArrayList<>();
                l.add(o);
                queryObservers.put(qID, l);
            }
        }
    }

    @Override
    public void register(ContinuousQueryExecution ceq, QueryResponseFormatter o) {
        String qID = ceq.getQueryID();
        log.info("Registering Observer [" + o.getClass() + "] to Query [" + qID + "]");
        if (!registeredQueries.containsKey(qID))
            throw new UnregisteredQueryExeception(qID);
        else {
            ceq.addFormatter(o);
            createQueryObserver(o, qID);
        }
    }

    @Override
    public void removeQueryResponseFormatter(QueryResponseFormatter queryResponseFormatter) {

    }

}
