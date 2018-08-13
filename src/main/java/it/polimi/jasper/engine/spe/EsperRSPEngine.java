package it.polimi.jasper.engine.spe;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.time.CurrentTimeEvent;
import it.polimi.jasper.engine.spe.esper.EsperStreamRegistrationService;
import it.polimi.jasper.engine.spe.esper.RuntimeManager;
import it.polimi.jasper.engine.streaming.RegisteredRDFStream;
import it.polimi.jasper.engine.streaming.items.StreamItem;
import it.polimi.jasper.engine.windowing.EsperWindowAssigner;
import it.polimi.yasper.core.engine.RSPEngine;
import it.polimi.yasper.core.quering.ContinuousQuery;
import it.polimi.yasper.core.quering.SDS;
import it.polimi.yasper.core.quering.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.quering.formatter.QueryResponseFormatter;
import it.polimi.yasper.core.reasoning.Entailment;
import it.polimi.yasper.core.spe.windowing.assigner.WindowAssigner;
import it.polimi.yasper.core.stream.rdf.RDFStream;
import it.polimi.yasper.core.stream.schema.StreamSchema;
import it.polimi.yasper.core.utils.EngineConfiguration;
import lombok.extern.log4j.Log4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
public abstract class EsperRSPEngine implements RSPEngine<StreamItem> {

    protected Map<String, WindowAssigner> stream_dispatching_service;
    protected Map<String, SDS> assignedSDS;
    protected Map<String, ContinuousQueryExecution> queryExecutions;
    protected Map<String, ContinuousQuery> registeredQueries;
    protected Map<String, List<QueryResponseFormatter>> queryObservers;
    protected HashMap<String, Entailment> entailments;

    protected EsperStreamRegistrationService stream_registration_service;

    protected EngineConfiguration rsp_config;

    private final RuntimeManager manager;
    private final EPServiceProvider cep;
    private final EPRuntime runtime;
    protected final EPAdministrator admin;

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

        log.debug("Running Configuration ]");
        log.debug("Event Time [" + this.rsp_config.isUsingEventTime() + "]");
        log.debug("Partial Window [" + this.rsp_config.partialWindowsEnabled() + "]");
        log.debug("Query Recursion [" + this.rsp_config.isRecursionEnables() + "]");
        log.debug("Query Class [" + this.rsp_config.getQueryClass() + "]");
        log.debug("StreamItem Class [" + this.rsp_config.getStreamSchema() + "]");

        runtime.sendEvent(new CurrentTimeEvent(t0));
    }

    @Override
    public RDFStream register(RDFStream s) {
        EPStatement epl = stream_registration_service.register(s);
        RegisteredRDFStream value = new RegisteredRDFStream(s.getURI(), s, epl, this);
        return value;
    }

    @Override
    public void unregister(RDFStream s) {
        stream_registration_service.unregister(s);
    }

    @Override
    public boolean process(StreamItem e) {
        String streamURI = e.getStreamURI();
        if (stream_dispatching_service.containsKey(streamURI)) {
            stream_dispatching_service.get(streamURI).notify(e);
            return true;
        }
        return false;
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

    protected void save(ContinuousQuery q, ContinuousQueryExecution cqe, SDS sds) {
        String id = q.getID();
        registeredQueries.put(id, q);
        queryObservers.put(id, new ArrayList<>());
        assignedSDS.put(id, sds);
        queryExecutions.put(id, cqe);
    }

}
