package it.polimi.jasper.rspql.tvg;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.StatementAwareUpdateListener;
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
public abstract class EsperTimeVaryingGraph extends Observable implements StatementAwareUpdateListener, TimeVarying<Graph> {

    protected Report report;
    protected EsperWindowAssigner wa;
    protected Maintenance maintenance;
    protected Graph graph;
    protected long now;

    protected ContentGraphBean c;

    public EsperTimeVaryingGraph(Graph content, Maintenance maintenance, Report report, EsperWindowAssigner wa) {
        this.maintenance = maintenance;
        this.wa = wa;
        this.report = report;
        this.graph = content;
    }

    @Override
    public synchronized void update(EventBean[] newData, EventBean[] oldData, EPStatement stmt, EPServiceProvider eps) {

        if (!wa.getStatement().equals(stmt))
            throw new RuntimeException("Different Update Statement");

        long event_time = eps.getEPRuntime().getCurrentTime();

        long systime = System.currentTimeMillis();

        this.c = Maintenance.NAIVE.equals(maintenance)
                ? new ContentGraphBean(graph, newData)
                : new IncrementalContentGraphBean(graph, newData, oldData);

        this.c.setLast_timestamp_changed(event_time);

        if (report.report(null, c, event_time, systime)) {
            log.info("[" + Thread.currentThread() + "][" + systime + "] FROM STATEMENT: " + stmt.getText() + " AT "
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
        graph = wa.getContent(ts).coalesce();
    }

    @Override
    public Graph get() {
        return graph;
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
