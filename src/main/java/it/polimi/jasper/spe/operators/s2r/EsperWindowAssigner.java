package it.polimi.jasper.spe.operators.s2r;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.soda.EPStatementObjectModel;
import com.espertech.esper.common.client.util.SafeIterator;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import it.polimi.jasper.rspql.tvg.EsperTimeVaryingGraphImpl;
import it.polimi.jasper.rspql.tvg.NamedEsperTimeVaryingGraph;
import it.polimi.jasper.spe.content.ContentGraphBean;
import it.polimi.jasper.spe.esper.EsperTime;
import it.polimi.jasper.spe.esper.RuntimeManager;
import it.polimi.jasper.streams.items.GraphStreamItem;
import it.polimi.yasper.core.rspql.timevarying.TimeVarying;
import it.polimi.yasper.core.spe.content.Content;
import it.polimi.yasper.core.spe.content.Maintenance;
import it.polimi.yasper.core.spe.operators.r2r.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.spe.operators.s2r.execution.assigner.WindowAssigner;
import it.polimi.yasper.core.spe.operators.s2r.execution.instance.Window;
import it.polimi.yasper.core.spe.report.Report;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.tick.Tick;
import it.polimi.yasper.core.spe.time.Time;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.reasoner.Reasoner;

import java.util.List;
import java.util.Observable;
import java.util.Observer;

@Log4j
@Getter
public class EsperWindowAssigner implements WindowAssigner<Graph, Graph>, Observer {

    private final String stream_iri_encoded;
    private final boolean eventtime;
    private final EPDeployment deployment;
    private final String window_iri_encoded;
    private EPDeploymentService admin;
    private EPStatement statement;
    private EPEventService runtime;
    private Time time;
    private Report report;
    private Tick tick;
    private ReportGrain reportGrain = ReportGrain.SINGLE;

    @Setter
    @Getter
    private Reasoner reasoner;
    @Setter
    @Getter
    private Maintenance maintenance;

    public EsperWindowAssigner(String stream_iri_encoded, String window_iri_encoded, Tick tick, Report report, boolean event_time, Maintenance maintenance, EPStatementObjectModel stm) {
        this.stream_iri_encoded = stream_iri_encoded;
        this.window_iri_encoded = window_iri_encoded;
        this.tick = tick;
        this.report = report;
        this.eventtime = event_time;
        this.runtime = RuntimeManager.getEPRuntime();
        this.admin = RuntimeManager.getAdmin();
        this.time = new EsperTime(runtime);
        this.maintenance = maintenance;

        try {
            // Build compiler arguments
            CompilerArguments args = new CompilerArguments(RuntimeManager.getRuntimeConfiguration());

            // Make the existing EPL objects available to the compiler
            args.getPath().add(RuntimeManager.getCEP().getRuntimePath());

            EPCompiled compile = EPCompilerProvider.getCompiler().compile(stm.toEPL(), args);

            this.deployment = admin.deploy(compile);
            this.statement = admin.getStatement(deployment.getDeploymentId(), window_iri_encoded);

        } catch (EPDeployException | EPCompileException e) {
            throw new RuntimeException(e.getCause());
        }

    }

    @Override
    public Report report() {
        return report;
    }

    @Override
    public Tick tick() {
        return tick;
    }

    @Override
    public Time time() {
        return time;
    }

    @Override
    public Content<Graph> getContent(long now) {
        SafeIterator<EventBean> iterator = statement.safeIterator();
        ContentGraphBean events = new ContentGraphBean(new GraphMem());
        events.setLast_timestamp_changed(now);
        while (iterator.hasNext()) {
            events.add(iterator.next());
        }
        return events;
    }

    @Override
    public List<Content<Graph>> getContents(long now) {
        return null;
    }

    @Override
    public TimeVarying<Graph> set(ContinuousQueryExecution execution) {
        Graph content = reasoner != null ? reasoner.bind(new GraphMem()) : new GraphMem();
        EsperTimeVaryingGraphImpl n = named()
                ? new NamedEsperTimeVaryingGraph(stream_iri_encoded, content, maintenance, report, this)
                : new EsperTimeVaryingGraphImpl(content, maintenance, report, this);
        statement.addListener(n);
        n.addObserver(execution);
        return n;
    }

    @Override
    public void notify(Graph arg, long ts) {
        process(arg, ts);
    }

    public boolean process(Graph g, long now) {

        long appTime = time.getAppTime();

        if (appTime < now) {
            time.setAppTime(now);
            runtime.sendEventMap(new GraphStreamItem(now, g, stream_iri_encoded), stream_iri_encoded);
            return true;
        } else if (appTime == now) {
            runtime.sendEventMap(new GraphStreamItem(now, g, stream_iri_encoded), stream_iri_encoded);
            return true;
        } else
            return false;

    }

    @Override
    public void update(Observable o, Object arg) {
        GraphStreamItem arg1 = (GraphStreamItem) arg;
        process(arg1.getTypedContent(), eventtime ? arg1.getAppTimestamp() : arg1.getSysTimestamp());
    }

    @Override
    public String iri() {
        return stream_iri_encoded;
    }

    @Override
    public boolean named() {
        return stream_iri_encoded != null;
    }

    @Override
    public Content<Graph> compute(long l, Window window) {
        return null;
        //TODO
    }

}
