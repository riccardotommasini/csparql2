package it.polimi.jasper.engine;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import it.polimi.jasper.querying.Entailment;
import it.polimi.jasper.engine.esper.EsperStreamRegistrationService;
import it.polimi.jasper.secret.EsperTime;
import it.polimi.jasper.operators.s2r.RuntimeManager;
import it.polimi.jasper.secret.report.EsperCCReportStrategy;
import it.polimi.jasper.secret.report.EsperNECReportStrategy;
import it.polimi.jasper.secret.report.EsperWCReportStrategy;
import it.polimi.jasper.streams.EPLRDFStream;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.engine.features.QueryDeletionFeature;
import it.polimi.yasper.core.engine.features.StreamDeletionFeature;
import it.polimi.yasper.core.engine.features.StreamRegistrationFeature;
import it.polimi.yasper.core.enums.ReportGrain;
import it.polimi.yasper.core.enums.Tick;
import it.polimi.yasper.core.format.QueryResultFormatter;
import it.polimi.yasper.core.operators.s2r.execution.assigner.Assigner;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.secret.report.Report;
import it.polimi.yasper.core.secret.report.ReportImpl;
import it.polimi.yasper.core.secret.time.Time;
import it.polimi.yasper.core.stream.metadata.StreamSchema;
import it.polimi.yasper.core.stream.web.WebStream;
import lombok.extern.log4j.Log4j;
import org.apache.jena.reasoner.rulesys.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
public abstract class EsperRSPEngine implements StreamRegistrationFeature<EPLRDFStream, WebStream>, StreamDeletionFeature<EPLRDFStream>, QueryDeletionFeature {

    protected final boolean enabled_recursion;
    protected final String responseFormat;
    protected final Boolean usingEventTime;
    protected final Entailment entailment;
    protected List<Rule> rules = null;
    protected Report report;
    protected ReportGrain reportGrain;
    protected Tick tick;
    protected String tbox;

    protected final String base_uri;
    protected Map<String, Assigner> stream_dispatching_service;
    protected Map<String, SDS> assignedSDS;
    protected Map<String, ContinuousQueryExecution> queryExecutions;
    protected Map<String, ContinuousQuery> registeredQueries;
    protected Map<String, List<QueryResultFormatter>> queryObservers;

    protected EsperStreamRegistrationService stream_registration_service;

    protected EngineConfiguration rsp_config;

    private final RuntimeManager manager;
    private final EPServiceProvider cep;
    private final EPRuntime runtime;
    protected final EPAdministrator admin;
    protected final Time time;

    public EsperRSPEngine(long t0, EngineConfiguration configuration) {
        this.assignedSDS = new HashMap<>();
        this.registeredQueries = new HashMap<>();
        this.queryObservers = new HashMap<>();
        this.queryExecutions = new HashMap<>();
        this.rsp_config = configuration;

        StreamSchema.Factory.registerSchema(this.rsp_config.getStreamSchema());


        this.cep = RuntimeManager.getCEP();
        this.manager = RuntimeManager.getInstance();
        this.runtime = RuntimeManager.getEPRuntime();
        this.admin = RuntimeManager.getAdmin();


        stream_registration_service = new EsperStreamRegistrationService(admin);
        stream_dispatching_service = new HashMap<>();

        this.enabled_recursion = rsp_config.isRecursionEnables();
        this.responseFormat = rsp_config.getResponseFormat();

        this.usingEventTime = rsp_config.isUsingEventTime();
        this.reportGrain = rsp_config.getReportGrain();
        this.tick = rsp_config.getTick();

        String string = rsp_config.getString("jasper.entailment");

        if (string == null)
            this.entailment = Entailment.NONE;
        else {
            this.entailment = Entailment.valueOf(string);
            this.tbox = rsp_config.getString("rsp_engine.tbox_location");
            if (tbox == null) {
                throw new RuntimeException("Not Specified TBOX");
            }
        }

        this.report = new ReportImpl();

        this.base_uri = rsp_config.getString("rsp_engine.base_uri");

        if (rsp_config.getBoolean("rsp_engine.periodic")) {
        }
        if (rsp_config.getBoolean("rsp_engine.on_content_change")) {
            report.add(new EsperCCReportStrategy());
        }
        if (rsp_config.getBoolean("rsp_engine.non_empty_content")) {
            report.add(new EsperNECReportStrategy());
        }
        if (rsp_config.getBoolean("rsp_engine.on_window_close")) {
            report.add(new EsperWCReportStrategy());
        }

        if (Entailment.CUSTOM.equals(this.entailment)) {
            this.rules = Rule.rulesFromURL(rsp_config.getString("jasper.rules"));
        }

        log.debug("Running Configuration ]");
        log.debug("Event Time [" + this.rsp_config.isUsingEventTime() + "]");
        log.debug("Partial Window [" + this.rsp_config.partialWindowsEnabled() + "]");
        log.debug("Query Recursion [" + this.rsp_config.isRecursionEnables() + "]");
        log.debug("Query Class [" + this.rsp_config.getQueryClass() + "]");
        log.debug("StreamItem Class [" + this.rsp_config.getStreamSchema() + "]");

        this.time = new EsperTime(runtime, t0);

    }

    @Override
    public EPLRDFStream register(WebStream s) {
        return stream_registration_service.register(s);
    }

    @Override
    public void unregister(EPLRDFStream s) {
        stream_registration_service.unregister(s);
    }

    public void unregister_query(String id) {
        registeredQueries.remove(id);
        queryObservers.remove(id);
        assignedSDS.remove(id);
        queryExecutions.remove(id);
    }

    @Override
    public void unregister(ContinuousQuery q) {
        unregister_query(q.getID());
    }

    protected ContinuousQueryExecution save(ContinuousQuery q, SDS sds, ContinuousQueryExecution cqe) {
        String id = q.getID();
        registeredQueries.put(id, q);
        queryObservers.put(id, new ArrayList<>());
        assignedSDS.put(id, sds);
        queryExecutions.put(id, cqe);
        return cqe;
    }

}
