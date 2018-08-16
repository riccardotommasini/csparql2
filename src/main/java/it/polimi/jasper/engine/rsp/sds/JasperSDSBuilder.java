package it.polimi.jasper.engine.rsp.sds;

import it.polimi.jasper.engine.rsp.querying.execution.observer.ContinuousQueryExecutionFactory;
import it.polimi.jasper.engine.rsp.querying.formatter.ResponseFormatterFactory;
import it.polimi.jasper.engine.spe.esper.EsperStreamRegistrationService;
import it.polimi.jasper.engine.spe.windowing.EsperWindowOperator;
import it.polimi.yasper.core.enums.EntailmentType;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.exceptions.StreamRegistrationException;
import it.polimi.yasper.core.quering.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.quering.querying.ContinuousQuery;
import it.polimi.yasper.core.quering.rspql.sds.SDS;
import it.polimi.yasper.core.quering.rspql.sds.SDSBuilder;
import it.polimi.yasper.core.quering.rspql.tvg.TimeVarying;
import it.polimi.yasper.core.reasoning.Entailment;
import it.polimi.yasper.core.spe.report.Report;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.scope.Tick;
import it.polimi.yasper.core.spe.windowing.assigner.WindowAssigner;
import it.polimi.yasper.core.stream.RegisteredStream;
import it.polimi.yasper.core.utils.QueryConfiguration;
import it.polimi.yasper.core.utils.RDFUtils;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.compose.MultiUnion;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.InfModelImpl;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.riot.system.IRIResolver;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Riccardo on 05/09/2017.
 */
@Log4j
@RequiredArgsConstructor
public class JasperSDSBuilder implements SDSBuilder {

    private final QueryConfiguration queryConfiguration;

    private final HashMap<String, Entailment> entailments;

    private final IRIResolver resolver;

    private final Report report;
    private final String responseFormat;
    private final Boolean distinct = true;
    private final Boolean enabled_recursion;
    private final Boolean usingEventTime;
    private final ReportGrain reportGrain;
    private final Tick tick;

    private final EsperStreamRegistrationService stream_registration_service;
    private final Map<String, WindowAssigner> stream_dispatching_service;

    @Getter
    protected Reasoner reasoner;

    @Getter
    private JenaSDS sds;

    @Getter
    private ContinuousQueryExecution qe;

    private Maintenance maintenance;

    private String tboxLocation;
    private EntailmentType entailment;
    private Entailment ent;
    private Model tbox;

    @Override
    public void visit(ContinuousQuery query) {

        this.maintenance = queryConfiguration.getSdsMaintainance();

        this.tboxLocation = queryConfiguration.getTboxLocation();
        this.tbox = ModelFactory.createDefaultModel().read(tboxLocation);

        this.entailment = queryConfiguration.getReasoningEntailment();
        this.ent = entailments.get(entailment.name());

        this.reasoner = ContinuousQueryExecutionFactory.getGenericRuleReasoner(ent, tbox);

        if (query.isRecursive() && !this.enabled_recursion) {
            throw new UnsupportedOperationException("Recursion must be enabled");
        }

        this.sds = new JenaSDS(new MultiUnion());

        this.qe = ContinuousQueryExecutionFactory.createObserver(query, sds);

        if (query.isConstructType()) {
            this.qe.addFormatter(ResponseFormatterFactory.getConstructResponseSysOutFormatter(responseFormat, distinct));
        } else if (query.isSelectType()) {
            this.qe.addFormatter(ResponseFormatterFactory.getSelectResponseSysOutFormatter(responseFormat, distinct));
        } else {
            this.qe.addFormatter(ResponseFormatterFactory.getGenericResponseSysOutFormatter(responseFormat, distinct));
        }

        query.getNamedGraphURIs().forEach(g -> {
            Model m = ModelFactory.createDefaultModel();
            if (!query.getNamedwindowsURIs().contains(g)) {
                if (this.reasoner != null)
                    this.sds.addNamedModel(g, new InfModelImpl(this.reasoner
                            .bind(m.read(g).getGraph())));
                else {
                    this.sds.addNamedModel(g, m);
                }
            }
        });

        query.getGraphURIs().forEach(g -> {
            Model m = ModelFactory.createDefaultModel().read(g);
            if (this.reasoner != null)
                this.sds.getDefaultModel().add(new InfModelImpl(this.reasoner.bind(m.getGraph())));
            else
                this.sds.getDefaultModel().add(m);
        });

        Map<String, RegisteredStream> registeredStreams = stream_registration_service.getRegisteredStreams();

        query.getWindowMap().forEach((wo, s) -> {
            String key = this.resolver.resolveToString("streams/" + s.getURI());
            if (!registeredStreams.containsKey(key)) {
                throw new StreamRegistrationException(s.getURI());
            } else {

                EsperWindowOperator ewo = new EsperWindowOperator(
                        this.tick,
                        this.report,
                        this.reasoner,
                        this.usingEventTime,
                        this.reportGrain,
                        this.maintenance,
                        wo);

                WindowAssigner wa = ewo.apply(registeredStreams.get(key));

                this.stream_dispatching_service.put(key, wa);

                TimeVarying tvii = wa.set(qe);

                if (ewo.isNamed())
                    this.sds.add(RDFUtils.createIRI(wo.getName()), tvii);
                else
                    this.sds.add(tvii);

            }
        });

    }

    @Override
    public SDS getSDS() {
        return sds;
    }

    @Override
    public ContinuousQueryExecution getContinuousQueryExecution() {
        return qe;
    }

}
