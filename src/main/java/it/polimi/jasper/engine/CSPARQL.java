package it.polimi.jasper.engine;

import it.polimi.jasper.spe.operators.r2r.syntax.QueryFactory;
import it.polimi.jasper.rspql.reasoning.EntailmentImpl;
import it.polimi.jasper.rspql.reasoning.EntailmentType;
import it.polimi.jasper.rspql.reasoning.ReasoningUtils;
import it.polimi.jasper.rspql.sds.JasperSDSManager;
import it.polimi.jasper.spe.operators.r2r.syntax.RSPQLJenaQuery;
import it.polimi.jasper.spe.report.EsperNECReportStrategy;
import it.polimi.jasper.spe.report.EsperWCReportStrategy;
import it.polimi.yasper.core.engine.EngineConfiguration;
import it.polimi.yasper.core.engine.features.QueryRegistrationFeature;
import it.polimi.yasper.core.spe.operators.r2r.ContinuousQuery;
import it.polimi.yasper.core.spe.operators.r2r.QueryConfiguration;
import it.polimi.yasper.core.spe.operators.r2r.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.report.ReportImpl;
import it.polimi.yasper.core.spe.tick.Tick;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.riot.system.IRIResolver;

import java.io.IOException;
import java.util.HashMap;

@Log4j
public class CSPARQL extends EsperRSPEngine implements QueryRegistrationFeature<RSPQLJenaQuery> {

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
    public ContinuousQueryExecution register(RSPQLJenaQuery continuousQuery) {
        return register(continuousQuery, null);
    }


    @Override
    public ContinuousQueryExecution register(RSPQLJenaQuery q, QueryConfiguration c) {

        JasperSDSManager builder = new JasperSDSManager(q,
                entailments.get(EntailmentType.RDFS.name()),
                this.resolver,
                this.report,
                this.responseFormat,
                this.enabled_recursion,
                this.usingEventTime,
                this.reportGrain,
                this.tick,
                this.stream_registration_service,
                this.stream_dispatching_service, c.getSdsMaintainance(), c.getTboxLocation());

        return save(q, builder.build(), builder.getContinuousQueryExecution());

    }

}
