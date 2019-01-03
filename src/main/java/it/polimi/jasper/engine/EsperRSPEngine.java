package it.polimi.jasper.engine;

import com.espertech.esper.runtime.client.EPDeploymentService;
import com.espertech.esper.runtime.client.EPEventService;
import com.espertech.esper.runtime.client.EPRuntime;
import it.polimi.jasper.rspql.reasoning.Entailment;
import it.polimi.jasper.spe.esper.EsperStreamRegistrationService;
import it.polimi.jasper.spe.esper.RuntimeManager;
import it.polimi.jasper.streams.RegisteredEPLStream;
import it.polimi.yasper.core.engine.EngineConfiguration;
import it.polimi.yasper.core.engine.features.QueryDeletionFeature;
import it.polimi.yasper.core.engine.features.StreamDeletionFeature;
import it.polimi.yasper.core.engine.features.StreamRegistrationFeature;
import it.polimi.yasper.core.rspql.sds.SDS;
import it.polimi.yasper.core.spe.operators.r2r.ContinuousQuery;
import it.polimi.yasper.core.spe.operators.r2r.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;
import it.polimi.yasper.core.spe.operators.s2r.execution.assigner.WindowAssigner;
import it.polimi.yasper.core.spe.report.Report;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.tick.Tick;
import it.polimi.yasper.core.stream.rdf.RDFStream;
import it.polimi.yasper.core.stream.schema.StreamSchema;
import lombok.extern.log4j.Log4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
public abstract class EsperRSPEngine implements StreamRegistrationFeature<RegisteredEPLStream, RDFStream>, StreamDeletionFeature<RegisteredEPLStream>, QueryDeletionFeature {

    protected final boolean enabled_recursion;
    protected final String responseFormat;
    protected final Boolean usingEventTime;
    protected Report report;
    protected ReportGrain reportGrain;
    protected Tick tick;

    protected Map<String, WindowAssigner> stream_dispatching_service;
    protected Map<String, SDS> assignedSDS;
    protected Map<String, ContinuousQueryExecution> queryExecutions;
    protected Map<String, ContinuousQuery> registeredQueries;
    protected Map<String, List<QueryResultFormatter>> queryObservers;
    protected HashMap<String, Entailment> entailments;

    protected EsperStreamRegistrationService stream_registration_service;

    protected EngineConfiguration rsp_config;

    private final RuntimeManager manager;
    private final EPRuntime cep;
    private final EPEventService runtime;
    protected final EPDeploymentService admin;

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
        this.report = rsp_config.getReport();
        this.usingEventTime = rsp_config.isUsingEventTime();
        this.reportGrain = rsp_config.getReportGrain();
        this.tick = rsp_config.getTick();

        log.debug("Running Configuration ]");
        log.debug("Event Time [" + this.rsp_config.isUsingEventTime() + "]");
        log.debug("Partial Window [" + this.rsp_config.partialWindowsEnabled() + "]");
        log.debug("Query Recursion [" + this.rsp_config.isRecursionEnables() + "]");
        log.debug("Query Class [" + this.rsp_config.getQueryClass() + "]");
        log.debug("StreamItem Class [" + this.rsp_config.getStreamSchema() + "]");

        runtime.clockExternal();
        runtime.advanceTime(t0);
    }

    @Override
    public RegisteredEPLStream register(RDFStream s) {
        return stream_registration_service.register(s);
    }

    @Override
    public void unregister(RegisteredEPLStream s) {
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
