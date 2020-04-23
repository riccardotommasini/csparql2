package it.polimi.jasper.operators.s2r;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import it.polimi.jasper.sds.EsperTimeVaryingGeneric;
import it.polimi.jasper.sds.tvg.EsperTimeVaryingGraphImpl;
import it.polimi.jasper.sds.tvg.NamedEsperTimeVaryingGraph;
import it.polimi.jasper.secret.content.ContentEventBean;
import it.polimi.jasper.secret.content.JenaGraphContent;
import it.polimi.jasper.streams.items.GraphStreamItem;
import it.polimi.yasper.core.enums.Maintenance;
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
import org.apache.jena.graph.Graph;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.reasoner.Reasoner;

import java.util.List;
import java.util.Observable;

@Log4j
@Getter
public class EsperGGWindowAssigner extends AbstractEsperWindowAssigner<Graph, Graph> {

    @Setter
    @Getter
    private Reasoner reasoner;
    @Setter
    @Getter
    private Maintenance maintenance;

    public EsperGGWindowAssigner(String name, Tick tick, Report report, boolean event_time, Maintenance maintenance, EPStatementObjectModel stm, Time time) {
        super(name, tick, report, event_time, stm, time);
        this.maintenance = maintenance;
    }

    public Content<Graph, Graph> getContent(long now) {
        SafeIterator<EventBean> iterator = statement.safeIterator();
        JenaGraphContent events = new JenaGraphContent(new GraphMem());
        events.setLast_timestamp_changed(now);
        while (iterator.hasNext()) {
            events.add(iterator.next());
        }
        return events;
    }

    @Override
    public List<Content<Graph, Graph>> getContents(long now) {
        return null;
    }

    @Override
    public TimeVarying<Graph> set(ContinuousQueryExecution execution) {
        Graph content = reasoner != null ? reasoner.bind(new GraphMem()) : new GraphMem();
        ContentEventBean<Graph, Graph, Graph> c = new JenaGraphContent(content);
        Assigner<Graph, Graph> wo = this;
        EsperTimeVaryingGeneric<Graph, Graph> n = named()
                ? new NamedEsperTimeVaryingGraph(c, name, maintenance, report, wo, execution.getSDS())
                : new EsperTimeVaryingGraphImpl(c, maintenance, report, wo, execution.getSDS());
        statement.addListener(n);
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
            runtime.sendEvent(new GraphStreamItem(now, g, name), name);
            return true;
        } else if (appTime == now) {
            runtime.sendEvent(new GraphStreamItem(now, g, name), name);
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
    public Content<Graph, Graph> compute(long l, Window window) {
        return null;
        //TODO
    }

}
