package it.polimi.seraph.s2r;

import com.espertech.esper.client.*;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import it.polimi.jasper.operators.s2r.RuntimeManager;
import it.polimi.seraph.sds.EsperTimeVaryingPGraphImpl;
import it.polimi.seraph.sds.NamedEsperTimeVaryingPGraph;
import it.polimi.seraph.content.ContentPGraphBean;
import it.polimi.seraph.streans.items.PGraphStreamItem;
import it.polimi.seraph.streans.PGraph;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.enums.ReportGrain;
import it.polimi.yasper.core.enums.Tick;
import it.polimi.yasper.core.operators.s2r.execution.assigner.Assigner;
import it.polimi.yasper.core.operators.s2r.execution.instance.Window;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.timevarying.TimeVarying;
import it.polimi.yasper.core.secret.content.Content;
import it.polimi.yasper.core.secret.report.Report;
import it.polimi.yasper.core.secret.time.Time;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.reasoner.Reasoner;

import java.util.List;
import java.util.Observable;
import java.util.Observer;

@Log4j
@Getter
public class EsperWindowAssignerPGraph implements Assigner<PGraph, PGraph>, Observer {

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

    public EsperWindowAssignerPGraph(String name, Tick tick, Report report, boolean event_time, Maintenance maintenance, EPStatementObjectModel stm, Time time) {
        this.name = name;
        this.tick = tick;
        this.report = report;
        this.eventtime = event_time;
        this.runtime = RuntimeManager.getEPRuntime();
        this.admin = RuntimeManager.getAdmin();
        this.statement = admin.create(stm, name);
        this.maintenance = maintenance;
        this.time = time;
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
    public Content<PGraph> getContent(long now) {
        SafeIterator<EventBean> iterator = statement.safeIterator();
        ContentPGraphBean events = new ContentPGraphBean(new PGraph());
        events.setLast_timestamp_changed(now);
        while (iterator.hasNext()) {
            events.add(iterator.next());
        }
        return events;
    }

    @Override
    public List<Content<PGraph>> getContents(long now) {
        return null;
    }

    @Override
    public TimeVarying<PGraph> set(ContinuousQueryExecution execution) {
        PGraph content = new PGraph();
        EsperTimeVaryingPGraphImpl n = named()
                ? new NamedEsperTimeVaryingPGraph(name, content, maintenance, report, this)
                : new EsperTimeVaryingPGraphImpl(content, maintenance, report, this);
        statement.addListener(n);
        n.addListener(execution);
        //TODO
        return n;

    }

    @Override
    public void notify(PGraph arg, long ts) {
        process(arg, ts);
    }

    public boolean process(PGraph g, long now) {

        long appTime = time.getAppTime();

        if (appTime < now) {
            time.setAppTime(now);
            runtime.sendEvent(new PGraphStreamItem(now, g, name), name);
            return true;
        } else if (appTime == now) {
            runtime.sendEvent(new PGraphStreamItem(now, g, name), name);
            return true;
        } else
            return false;

    }

    @Override
    public void update(Observable o, Object arg) {
        PGraphStreamItem arg1 = (PGraphStreamItem) arg;
        process(arg1.getTypedContent(), eventtime ? arg1.getAppTimestamp() : arg1.getSysTimestamp());
    }

    @Override
    public String iri() {
        return name;
    }

    @Override
    public boolean named() {
        return name != null;
    }

    @Override
    public Content<PGraph> compute(long l, Window window) {
        return null;
        //TODO
    }

}
