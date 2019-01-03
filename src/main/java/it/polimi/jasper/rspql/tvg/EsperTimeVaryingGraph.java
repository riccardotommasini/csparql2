package it.polimi.jasper.rspql.tvg;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import it.polimi.jasper.spe.content.ContentGraphBean;
import it.polimi.jasper.spe.content.IncrementalContentGraphBean;
import it.polimi.jasper.spe.operators.s2r.EsperWindowAssigner;
import it.polimi.yasper.core.rspql.timevarying.TimeVarying;
import it.polimi.yasper.core.spe.content.Maintenance;
import it.polimi.yasper.core.spe.report.Report;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

import java.util.Observable;
import java.util.Observer;

/**
 * Created by riccardo on 05/07/2017.
 */
@Log4j
@Getter
public abstract class EsperTimeVaryingGraph extends Observable implements UpdateListener, TimeVarying<Graph> {

    protected Report report;
    protected EsperWindowAssigner wa;
    protected Maintenance maintenance;
    protected long now;

    protected ContentGraphBean c;

    public EsperTimeVaryingGraph(Graph content, Maintenance maintenance, Report report, EsperWindowAssigner wa) {
        this.maintenance = maintenance;
        this.wa = wa;
        this.report = report;

        this.c = Maintenance.NAIVE.equals(maintenance)
                ? new ContentGraphBean(content)
                : new IncrementalContentGraphBean(content);

    }

    @Override
    public synchronized void update(EventBean[] newData, EventBean[] oldData, EPStatement stmt, EPRuntime runtime) {

        if (!wa.getStatement().equals(stmt))
            throw new RuntimeException("Different Update Statement");

        long event_time = runtime.getEventService().getCurrentTime();

        long systime = System.currentTimeMillis();

        this.c.update(newData, oldData, event_time);

        if (report.report(null, c, event_time, systime)) {
            log.debug("[" + Thread.currentThread() + "][" + systime + "] FROM STATEMENT: " + stmt.toString() + " AT "
                    + event_time);

            setChanged();
            notifyObservers(event_time);
        }

    }

    @Override
    public synchronized void addObserver(Observer o) {
        super.addObserver(o);
    }

    @Override
    public void materialize(long ts) {
        if (this.c.getTimeStampLastUpdate() < ts) {
            this.c.replace(this.wa.getContent(ts).coalesce());
        } else
            this.c.coalesce();
    }

    @Override
    public Graph get() {
        return c.coalesce();
    }

    @Override
    public String iri() {
        return "";
    }

    @Override
    public boolean named() {
        return false;
    }

}
