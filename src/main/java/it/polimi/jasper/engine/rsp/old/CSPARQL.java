package it.polimi.jasper.engine.rsp.old;

import it.polimi.jasper.engine.rsp.EsperRSPEngine;
import it.polimi.jasper.engine.rsp.querying.syntax.QueryFactory;
import it.polimi.jasper.engine.rsp.reasoning.EntailmentImpl;
import it.polimi.jasper.engine.rsp.reasoning.ReasoningUtils;
import it.polimi.jasper.engine.rsp.sds.JasperSDSBuilder;
import it.polimi.jasper.engine.spe.report.EsperNECReportStrategy;
import it.polimi.jasper.engine.spe.report.EsperWCReportStrategy;
import it.polimi.yasper.core.enums.EntailmentType;
import it.polimi.yasper.core.exceptions.UnregisteredQueryExeception;
import it.polimi.yasper.core.quering.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.quering.formatter.QueryResponseFormatter;
import it.polimi.yasper.core.quering.querying.ContinuousQuery;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.report.ReportImpl;
import it.polimi.yasper.core.spe.scope.Tick;
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

@Log4j
public class CSPARQL extends EsperRSPEngine {

    @Getter
    private final IRIResolver resolver;

    public CSPARQL(long t0, EngineConfiguration configuration) {
        super(t0, configuration);
        this.resolver = IRIResolver.create(configuration.getBaseURI());

        this.entailments = new HashMap<>();

        //CSPARQL

        this.report = new ReportImpl();
        this.report.add(new EsperWCReportStrategy());
        this.report.add(new EsperNECReportStrategy());

        this.tick = Tick.TIME_DRIVEN;

        this.reportGrain = ReportGrain.SINGLE;

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
        JasperSDSBuilder builder = new JasperSDSBuilder(
                c,
                this.entailments,
                this.resolver,
                this.report,
                this.responseFormat,
                this.enabled_recursion,
                this.usingEventTime,
                this.reportGrain,
                this.tick,
                this.stream_registration_service,
                this.stream_dispatching_service);

        builder.visit(q);

        return save(q, builder.getContinuousQueryExecution(), builder.getSDS());

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
