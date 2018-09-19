package it.polimi.jasper.rspql.sds;

import it.polimi.jasper.rspql.reasoning.Entailment;
import it.polimi.jasper.spe.esper.EsperStreamRegistrationService;
import it.polimi.jasper.spe.operators.r2r.execution.ContinuousQueryExecutionFactory;
import it.polimi.jasper.spe.operators.r2r.syntax.RSPQLJenaQuery;
import it.polimi.jasper.spe.operators.r2s.formatter.ResponseFormatterFactory;
import it.polimi.jasper.spe.operators.s2r.EsperWindowOperator;
import it.polimi.yasper.core.engine.exceptions.StreamRegistrationException;
import it.polimi.yasper.core.rspql.RDFUtils;
import it.polimi.yasper.core.rspql.sds.SDS;
import it.polimi.yasper.core.rspql.sds.SDSManager;
import it.polimi.yasper.core.rspql.timevarying.TimeVarying;
import it.polimi.yasper.core.spe.content.Maintenance;
import it.polimi.yasper.core.spe.operators.r2r.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.spe.operators.s2r.execution.assigner.WindowAssigner;
import it.polimi.yasper.core.spe.report.Report;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.tick.Tick;
import it.polimi.yasper.core.stream.RegisteredStream;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.compose.MultiUnion;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.InfModelImpl;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.riot.system.IRIResolver;

import java.util.Map;

/**
 * Created by Riccardo on 05/09/2017.
 */
@Log4j
public class JasperSDSManager implements SDSManager {

    private final RSPQLJenaQuery query;

    private final Entailment ent;

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
    private ContinuousQueryExecution cqe;

    private Maintenance maintenance;

    private String tboxLocation;
    private Model tbox;

    public JasperSDSManager(RSPQLJenaQuery query, Entailment ent, IRIResolver resolver, Report report, String responseFormat, Boolean enabled_recursion, Boolean usingEventTime, ReportGrain reportGrain, Tick tick, EsperStreamRegistrationService stream_registration_service, Map<String, WindowAssigner> stream_dispatching_service, Maintenance sdsMaintainance, String tboxLocation) {
        this.query = query;
        this.ent = ent;
        this.resolver = resolver;
        this.report = report;
        this.responseFormat = responseFormat;
        this.enabled_recursion = enabled_recursion;
        this.usingEventTime = usingEventTime;
        this.reportGrain = reportGrain;
        this.tick = tick;
        this.stream_registration_service = stream_registration_service;
        this.stream_dispatching_service = stream_dispatching_service;
    }

    @Override
    public SDS build() {

        this.tbox = ModelFactory.createDefaultModel().read(tboxLocation);

        this.reasoner = ContinuousQueryExecutionFactory.getGenericRuleReasoner(ent, tbox);

        if (query.isRecursive() && !this.enabled_recursion) {
            throw new UnsupportedOperationException("Recursion must be enabled");
        }

        this.sds = new JenaSDS(new MultiUnion());

        this.cqe = ContinuousQueryExecutionFactory.create(query, sds);

        if (query.isConstructType()) {
            this.cqe.add(ResponseFormatterFactory.getConstructResponseSysOutFormatter(responseFormat, distinct));
        } else if (query.isSelectType()) {
            this.cqe.add(ResponseFormatterFactory.getSelectResponseSysOutFormatter(responseFormat, distinct));
        } else {
            this.cqe.add(ResponseFormatterFactory.getGenericResponseSysOutFormatter(responseFormat, distinct));
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

        Map<String, RegisteredStream<Graph>> registeredStreams = stream_registration_service.getRegisteredStreams();

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

                WindowAssigner<Graph, Graph> wa = ewo.apply(registeredStreams.get(key));

                this.stream_dispatching_service.put(key, wa);

                TimeVarying<Graph> tvii = wa.set(cqe);

                if (ewo.named())
                    this.sds.add(RDFUtils.createIRI(wo.iri()), tvii);
                else
                    this.sds.add(tvii);

            }
        });

        return sds;

    }

    @Override
    public ContinuousQueryExecution getContinuousQueryExecution() {
        return cqe;
    }

}
