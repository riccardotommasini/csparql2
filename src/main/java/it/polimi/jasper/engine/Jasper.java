package it.polimi.jasper.engine;

import it.polimi.jasper.querying.syntax.QueryFactory;
import it.polimi.jasper.querying.syntax.RSPQLJenaQuery;
import it.polimi.jasper.sds.JasperSDSManager;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.engine.features.QueryObserverRegistrationFeature;
import it.polimi.yasper.core.engine.features.QueryRegistrationFeature;
import it.polimi.yasper.core.engine.features.QueryStringRegistrationFeature;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.enums.ReportGrain;
import it.polimi.yasper.core.exceptions.UnregisteredQueryExeception;
import it.polimi.yasper.core.format.QueryResultFormatter;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;
import it.polimi.yasper.core.secret.report.ReportImpl;
import it.polimi.yasper.core.secret.report.strategies.ReportingStrategy;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.engine.binding.Binding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Log4j
public class Jasper extends EsperRSPEngine<Graph> implements QueryObserverRegistrationFeature, QueryRegistrationFeature<RSPQLJenaQuery>, QueryStringRegistrationFeature {

    @Getter
    private IRIResolver resolver;
    private Maintenance maintenance;


    public Jasper(long t0, EngineConfiguration configuration) {
        super(t0, configuration);

//        String string = configuration.
//
//        if (string == null)
//            this.entailment = Entailment.NONE;
//        else {
//            this.entailment = Entailment.valueOf(string);
//            this.tbox = rsp_config.getString("rsp_engine.tbox_location");
//            if (tbox == null) {
//                throw new RuntimeException("Not Specified TBOX");
//            }
//        }
//
//        if (Entailment.CUSTOM.equals(this.entailment)) {
//            this.rules = Rule.rulesFromURL(rsp_config.getString("jasper.rules"));
//        }


                this.resolver = IRIResolver.create(base_uri);
        this.reportGrain = ReportGrain.SINGLE;
        this.maintenance = Maintenance.NAIVE;
    }

    public void setReport(ReportingStrategy... sr) {
        this.report = new ReportImpl();
        Arrays.stream(sr).forEach(this.report::add);
    }

    @Override
    public ContinuousQueryExecution<Graph, Graph, Binding> register(RSPQLJenaQuery continuousQuery) {
        return register(continuousQuery, SDSConfiguration.getDefault());
    }

    @Override
    public ContinuousQueryExecution<Graph, Graph, Binding> register(String s) {
        return register(s, SDSConfiguration.getDefault());
    }

    @Override
    public ContinuousQueryExecution<Graph, Graph, Binding> register(String q, SDSConfiguration queryConfiguration) {
        log.info("Parsing Query [" + q + "]");
        try {
            return register(QueryFactory.parse(resolver, q), queryConfiguration);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public ContinuousQueryExecution<Graph, Graph, Binding> register(RSPQLJenaQuery q, SDSConfiguration c) {

        JasperSDSManager builder = new JasperSDSManager(
                this,
                q,
                this.time,
                this.resolver,
                this.report,
                this.responseFormat,
                this.enabled_recursion,
                this.usingEventTime,
                this.reportGrain,
                this.tick,
                this.stream_registration_service,
                this.maintenance,
                this.tbox,
                this.entailment,
                this.rules);


        return save(q, builder.build(), builder.getContinuousQueryExecution());
    }

    @Override
    public void register(ContinuousQuery q, QueryResultFormatter f) {
        String qID = q.getID();
        log.info("Registering Observer [" + f.getClass() + "] to Query [" + qID + "]");
        if (!registeredQueries.containsKey(qID))
            throw new UnregisteredQueryExeception(qID);
        else {
            ContinuousQueryExecution ceq = queryExecutions.get(qID);
            ceq.add(f);
            createQueryObserver(f, qID);
        }
    }

    private void createQueryObserver(QueryResultFormatter o, String qID) {
        if (queryObservers.containsKey(qID)) {
            List<QueryResultFormatter> l = queryObservers.get(qID);
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
