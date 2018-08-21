package it.polimi.jasper.engine.spe.windowing;

import com.espertech.esper.client.*;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import it.polimi.jasper.engine.rsp.streams.items.GraphStreamItem;
import it.polimi.jasper.engine.spe.content.ContentGraphBean;
import it.polimi.jasper.engine.spe.esper.EsperTime;
import it.polimi.jasper.engine.spe.esper.RuntimeManager;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.quering.execution.ContinuousQueryExecution;
import it.polimi.yasper.core.quering.rspql.tvg.TimeVarying;
import it.polimi.yasper.core.spe.content.Content;
import it.polimi.yasper.core.spe.content.viewer.View;
import it.polimi.yasper.core.spe.report.Report;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.scope.Tick;
import it.polimi.yasper.core.spe.time.Time;
import it.polimi.yasper.core.spe.windowing.assigner.WindowAssigner;
import it.polimi.yasper.core.utils.EncodingUtils;
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
public class EsperWindowAssigner implements WindowAssigner<Graph>, Observer {

    private final String name;
    private final boolean eventtime;
    private EPAdministrator admin;
    private EPStatement statement;
    private EPRuntime runtime;
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

    public EsperWindowAssigner(String name, Tick tick, Report report, boolean event_time, EPStatementObjectModel stm) {
        this.name = name;
        this.tick = tick;
        this.report = report;
        this.eventtime = event_time;
        this.runtime = RuntimeManager.getEPRuntime();
        this.admin = RuntimeManager.getAdmin();
        this.statement = admin.create(stm, name);
        this.time = new EsperTime(runtime);
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
    public Content<Graph> getContent(long now) {
        SafeIterator<EventBean> iterator = statement.safeIterator();
        ContentGraphBean events = new ContentGraphBean();
        events.setLast_timestamp_changed(now);
        while (iterator.hasNext()) {
            events.add(iterator.next());
        }
        return events;
    }

    @Override
    public List<Content> getContents(long now) {
        return null;
    }

    @Override
    public void report(Report report) {
        this.report = report;
    }

    @Override
    public void tick(Tick tick) {
        this.tick = tick;
    }

    @Override
    public TimeVarying set(View v) {
        Graph content = reasoner != null ? reasoner.bind(new GraphMem()) : new GraphMem();
        EsperTimeVaryingGraphImpl n = isNamed()
                ? new NamedEsperTimeVaryingGraph(name, content, maintenance, report, this)
                : new EsperTimeVaryingGraphImpl(content, maintenance, report, this);
        statement.addListener(n);
        v.observerOf(this);
        return n;
    }

    @Override
    public TimeVarying set(ContinuousQueryExecution execution) {
        Graph content = reasoner != null ? reasoner.bind(new GraphMem()) : new GraphMem();
        EsperTimeVaryingGraphImpl n = isNamed()
                ? new NamedEsperTimeVaryingGraph(name, content, maintenance, report, this)
                : new EsperTimeVaryingGraphImpl(content, maintenance, report, this);
        statement.addListener(n);
        n.addObserver(execution);
        return n;
    }

    @Override
    public void report_grain(ReportGrain aw) {
        this.reportGrain = aw;
    }

    @Override
    public void notify(Graph arg, long ts) {
        process(arg, ts);
    }

    public boolean process(Graph g, long now) {

        if (time.getAppTime() <= now) {
            time.setAppTime(now);
        }

        runtime.sendEvent(new GraphStreamItem(now, g, name), name);
        return true;
    }

    @Override
    public void update(Observable o, Object arg) {
        GraphStreamItem arg1 = (GraphStreamItem) arg;
        process(arg1.getTypedContent(), eventtime ? arg1.getAppTimestamp() : arg1.getSysTimestamp());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isNamed() {
        return name != null;
    }

}
