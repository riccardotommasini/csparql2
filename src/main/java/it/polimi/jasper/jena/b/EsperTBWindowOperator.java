package it.polimi.jasper.jena.b;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;
import it.polimi.jasper.operators.s2r.AbstractEsperWindowAssigner;
import it.polimi.jasper.operators.s2r.epl.EPLFactory;
import it.polimi.jasper.sds.tv.EsperTimeVaryingGeneric;
import it.polimi.jasper.secret.content.ContentEventBean;
import it.polimi.jasper.utils.EncodingUtils;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.enums.ReportGrain;
import it.polimi.yasper.core.enums.Tick;
import it.polimi.yasper.core.operators.s2r.StreamToRelationOperator;
import it.polimi.yasper.core.operators.s2r.execution.instance.Window;
import it.polimi.yasper.core.operators.s2r.syntax.WindowNode;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.sds.timevarying.TimeVarying;
import it.polimi.yasper.core.secret.content.Content;
import it.polimi.yasper.core.secret.report.Report;
import it.polimi.yasper.core.secret.time.Time;
import it.polimi.yasper.core.stream.data.WebDataStream;
import lombok.RequiredArgsConstructor;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;

import java.util.List;
import java.util.Observable;

@RequiredArgsConstructor
public class EsperTBWindowOperator implements StreamToRelationOperator<Triple, BindingSet> {

    private final Tick tick;
    private final Report report;
    private final Boolean eventtime;
    private final ReportGrain reportGrain;
    private final Maintenance maintenance;
    private final Time time;
    private final WindowNode wo;
    private final SDS<BindingSet> sds;

    private final String name;

    @Override
    public String iri() {
        return wo.iri();
    }

    @Override
    public boolean named() {
        return wo.named();
    }

    @Override
    public TimeVarying<BindingSet> apply(WebDataStream<Triple> s) {
        EPStatement windowAssignerTB = EPLFactory.getWindowAssignerTB(tick, maintenance, report, eventtime, s.getURI(), wo.getStep(), wo.getRange(), wo.getUnitStep(), wo.getUnitRange(), wo.getType(), time);
        EsperTBWindowAssigner consumer = new EsperTBWindowAssigner(EncodingUtils.encode(s.getURI()), windowAssignerTB);
        s.addConsumer(consumer);
        return consumer.set(sds);
    }

    class EsperTBWindowAssigner extends AbstractEsperWindowAssigner<Triple, BindingSet> {

        public EsperTBWindowAssigner(String name, EPStatement stm) {
            super(name, EsperTBWindowOperator.this.tick, EsperTBWindowOperator.this.report, EsperTBWindowOperator.this.eventtime, stm, EsperTBWindowOperator.this.time);
        }

        public Content<Triple, BindingSet> getContent(long now) {
            SafeIterator<EventBean> iterator = statement.safeIterator();
            ContentEventBean<Triple, Graph, BindingSet> content2 = new JenaBindingContent();
            content2.setLast_timestamp_changed(now);
            while (iterator.hasNext()) {
                content2.add(iterator.next());
            }
            return content2;
        }

        @Override
        public List<Content<Triple, BindingSet>> getContents(long now) {
            return null;
        }

        @Override
        public TimeVarying<BindingSet> set(SDS<BindingSet> sds) {
            EsperTimeVaryingGeneric<Triple, BindingSet> n = named()
                    ? new NamedEsperTimeVaryingBinding(new JenaBindingContent(), name, EsperTBWindowOperator.this.maintenance, report, this, sds)
                    : new EsperTimeVaryingBindingImpl(new JenaBindingContent(), EsperTBWindowOperator.this.maintenance, report, this, sds);
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
}
