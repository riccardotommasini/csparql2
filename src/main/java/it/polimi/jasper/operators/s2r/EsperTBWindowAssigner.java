package it.polimi.jasper.operators.s2r;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import it.polimi.jasper.sds.EsperTimeVaryingGeneric;
import it.polimi.jasper.sds.tvb.EsperTimeVaryingBindingImpl;
import it.polimi.jasper.sds.tvb.NamedEsperTimeVaryingBinding;
import it.polimi.jasper.secret.content.BindingSet;
import it.polimi.jasper.secret.content.ContentEventBean;
import it.polimi.jasper.secret.content.JenaBindingContent;
import it.polimi.jasper.streams.items.TripleStreamItem;
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
import org.apache.jena.graph.Triple;

import java.util.List;
import java.util.Observable;

@Log4j
@Getter
public class EsperTBWindowAssigner extends AbstractEsperWindowAssigner<Triple, BindingSet> {

    @Setter
    @Getter
    private Maintenance maintenance;

    public EsperTBWindowAssigner(String name, Tick tick, Report report, boolean event_time, Maintenance maintenance, EPStatementObjectModel stm, Time time) {
        super(name, tick, report, event_time, stm, time);
        this.maintenance = maintenance;
    }

    public Content<Triple,BindingSet> getContent(long now) {
        SafeIterator<EventBean> iterator = statement.safeIterator();
        ContentEventBean<Triple, Graph, BindingSet> content2 = new JenaBindingContent();
        content2.setLast_timestamp_changed(now);
        while (iterator.hasNext()) {
            content2.add(iterator.next());
        }
        return content2;
    }

    @Override
    public List<Content<Triple,BindingSet>> getContents(long now) {
        return null;
    }

    @Override
    public TimeVarying<BindingSet> set(ContinuousQueryExecution execution) {
        Assigner<Triple, BindingSet> esperTBWindowAssigner = this;
        ContentEventBean<Triple, Graph, BindingSet> content2 = new JenaBindingContent();
        EsperTimeVaryingGeneric<Triple, BindingSet> n = named()
                ? new NamedEsperTimeVaryingBinding(content2, name, maintenance, report, esperTBWindowAssigner, execution.getSDS())
                : new EsperTimeVaryingBindingImpl(content2, maintenance, report, esperTBWindowAssigner, execution.getSDS());
        statement.addListener(n);
        return n;
    }

    @Override
    public void notify(Triple arg, long ts) {
        process(arg, ts);
    }

    public boolean process(Triple g, long now) {

        long appTime = time.getAppTime();

        if (appTime < now) {
            time.setAppTime(now);
            runtime.sendEvent(new TripleStreamItem(now, g, name), name);
            return true;
        } else if (appTime == now) {
            runtime.sendEvent(new TripleStreamItem(now, g, name), name);
            return true;
        } else
            return false;

    }

    @Override
    public void update(Observable o, Object arg) {
        TripleStreamItem arg1 = (TripleStreamItem) arg;
        process(arg1.getTypedContent(), eventtime ? arg1.getAppTimestamp() : arg1.getSysTimestamp());
    }

    @Override
    public Content<Triple, BindingSet> compute(long l, Window window) {
        return null;
        //TODO
    }

}
