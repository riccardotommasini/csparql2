package it.polimi.jasper.engine;

import it.polimi.jasper.rspql.querying.syntax.QueryFactory;
import it.polimi.jasper.rspql.querying.syntax.RSPQLJenaQuery;
import it.polimi.jasper.rspql.reasoning.EntailmentImpl;
import it.polimi.jasper.rspql.reasoning.EntailmentType;
import it.polimi.jasper.rspql.reasoning.ReasoningUtils;
import it.polimi.jasper.rspql.sds.JasperSDSBuilder;
import it.polimi.yasper.core.engine.EngineConfiguration;
import it.polimi.yasper.core.engine.exceptions.UnregisteredQueryExeception;
import it.polimi.yasper.core.engine.features.QueryObserverRegistrationFeature;
import it.polimi.yasper.core.engine.features.QueryRegistrationFeature;
import it.polimi.yasper.core.engine.features.QueryStringRegistrationFeature;
import it.polimi.yasper.core.rspql.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.rspql.formatter.QueryResponseFormatter;
import it.polimi.yasper.core.rspql.querying.ContinuousQuery;
import it.polimi.yasper.core.rspql.querying.QueryConfiguration;
import it.polimi.yasper.core.spe.report.ReportGrain;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.riot.system.IRIResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Log4j
public class Jasper extends EsperRSPEngine implements QueryObserverRegistrationFeature, QueryRegistrationFeature<RSPQLJenaQuery>, QueryStringRegistrationFeature {

    @Getter
    private final IRIResolver resolver;

    public Jasper(long t0, EngineConfiguration configuration) {
        super(t0, configuration);
        this.resolver = IRIResolver.create(configuration.getBaseURI());

        this.entailments = new HashMap<>();

        this.reportGrain = ReportGrain.SINGLE;

        //Adding default entailments
        String ent = EntailmentType.RDFS.name();
        this.entailments.put(ent, new EntailmentImpl(ent, Rule.rulesFromURL(ReasoningUtils.RHODF_RULE_SET_RUNTIME), EntailmentType.RDFS));
        ent = EntailmentType.RHODF.name();
        this.entailments.put(ent, new EntailmentImpl(ent, Rule.rulesFromURL(ReasoningUtils.RHODF_RULE_SET_RUNTIME), EntailmentType.RHODF));

    }

    @Override
    public ContinuousQueryExecution register(RSPQLJenaQuery continuousQuery) {
        try {
            return register(continuousQuery, QueryConfiguration.getDefault());
        } catch (ConfigurationException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public ContinuousQueryExecution register(String s) {
        try {
            return register(s, QueryConfiguration.getDefault());
        } catch (ConfigurationException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public ContinuousQueryExecution register(String q, QueryConfiguration queryConfiguration) {
        log.info("Parsing Query [" + q + "]");
        try {
            return register(QueryFactory.parse(resolver, q), queryConfiguration);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public ContinuousQueryExecution register(RSPQLJenaQuery q, QueryConfiguration c) {
        JasperSDSBuilder builder = new JasperSDSBuilder(
                q,
                c,
                entailments.get(EntailmentType.RDFS.name()),
                this.resolver,
                this.report,
                this.responseFormat,
                this.enabled_recursion,
                this.usingEventTime,
                this.reportGrain,
                this.tick,
                this.stream_registration_service,
                this.stream_dispatching_service);

        return save(q, builder.build(), builder.getContinuousQueryExecution());
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
}
