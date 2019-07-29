package it.polimi.jasper.sds;

import it.polimi.jasper.engine.Jasper;
import it.polimi.jasper.engine.esper.EsperStreamRegistrationService;
import it.polimi.jasper.execution.ContinuousQueryExecutionFactory;
import it.polimi.jasper.execution.JenaContinuousQueryExecution;
import it.polimi.jasper.operators.s2r.EsperWindowOperator;
import it.polimi.jasper.querying.Entailment;
import it.polimi.jasper.querying.syntax.RSPQLJenaQuery;
import it.polimi.jasper.streams.EPLRDFStream;
import it.polimi.yasper.core.RDFUtils;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.enums.ReportGrain;
import it.polimi.yasper.core.enums.Tick;
import it.polimi.yasper.core.exceptions.StreamRegistrationException;
import it.polimi.yasper.core.operators.s2r.execution.assigner.Assigner;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.sds.SDSManager;
import it.polimi.yasper.core.sds.timevarying.TimeVarying;
import it.polimi.yasper.core.secret.report.Report;
import it.polimi.yasper.core.secret.time.Time;
import it.polimi.yasper.core.stream.data.WebDataStream;
import it.polimi.yasper.core.stream.web.WebStream;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.compose.MultiUnion;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.InfModelImpl;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.riot.system.IRIResolver;

import java.util.List;
import java.util.Map;

/**
 * Created by Riccardo on 05/09/2017.
 */
@Log4j
public class JasperSDSManager implements SDSManager {

    private final RSPQLJenaQuery query;

    private final IRIResolver resolver;

    private final Report report;
    private final String responseFormat;
    private final Boolean distinct = true;
    private final Boolean enabled_recursion;
    private final Boolean usingEventTime;
    private final ReportGrain reportGrain;
    private final Tick tick;

    private final EsperStreamRegistrationService stream_registration_service;
    private final Entailment et;
    private final List<Rule> rules;
    private final Time time;
    private final Jasper jasper;

    @Getter
    protected Reasoner reasoner;

    @Getter
    private JenaSDS sds;

    @Getter
    private JenaContinuousQueryExecution cqe;

    private Maintenance maintenance;

    private String tboxLocation;
    private EPLRDFStream out;

    public JasperSDSManager(Jasper jasper, RSPQLJenaQuery query, Time time, IRIResolver resolver, Report report, String responseFormat, Boolean enabled_recursion, Boolean usingEventTime, ReportGrain reportGrain, Tick tick, EsperStreamRegistrationService stream_registration_service, Map<String, Assigner> stream_dispatching_service, Maintenance sdsMaintainance, String tboxLocation, Entailment et, List<Rule> rules) {
        this.jasper = jasper;
        this.query = query;
        this.time = time;
        this.resolver = resolver;
        this.report = report;
        this.responseFormat = responseFormat;
        this.enabled_recursion = enabled_recursion;
        this.usingEventTime = usingEventTime;
        this.reportGrain = reportGrain;
        this.tick = tick;
        this.stream_registration_service = stream_registration_service;
        this.maintenance = sdsMaintainance;
        this.tboxLocation = tboxLocation;
        this.et = et;
        this.rules = rules;
    }

    @Override
    public SDS build() {

        this.reasoner = ContinuousQueryExecutionFactory.getReasoner(et, rules, tboxLocation);

        if (query.isRecursive() && !this.enabled_recursion) {
            throw new UnsupportedOperationException("Recursion must be enabled");
        }

        this.sds = new JenaSDS(new MultiUnion());

        //Load Static Knowledge
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

        Map<String, WebDataStream<Graph>> registeredStreams = stream_registration_service.getRegisteredStreams();


        WebStream outputStream = query.getOutputStream();

        this.out = jasper.register(outputStream);

        this.cqe = ContinuousQueryExecutionFactory.create(resolver, query, sds, out, null);

        query.getWindowMap().forEach((wo, s) -> {

            String key = this.resolver.resolveToString(s.getURI());
            if (!registeredStreams.containsKey(s.getURI())) {
                throw new StreamRegistrationException(s.getURI());
            } else {

                EsperWindowOperator ewo = new EsperWindowOperator(
                        this.tick,
                        this.report,
                        this.reasoner,
                        this.usingEventTime,
                        this.reportGrain,
                        this.maintenance,
                        this.time,
                        wo,
                        this.cqe);

                cqe.addS2R(ewo);

                TimeVarying<Graph> tvii = ewo.apply(registeredStreams.get(key));

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
