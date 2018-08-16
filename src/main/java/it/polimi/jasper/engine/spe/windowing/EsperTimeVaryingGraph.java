package it.polimi.jasper.engine.spe.windowing;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.StatementAwareUpdateListener;
import com.espertech.esper.event.map.MapEventBean;
import it.polimi.jasper.engine.rsp.streams.items.StreamItem;
import it.polimi.jasper.engine.spe.content.ContentBean;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.quering.rspql.tvg.TimeVarying;
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
public abstract class EsperTimeVaryingGraph extends Observable implements StatementAwareUpdateListener, TimeVarying {

    protected Report report;
    protected EsperWindowAssigner wa;
    protected Maintenance maintenance;
    protected Graph graph;
    protected long now;

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

        Long event_time = eps.getEPRuntime().getCurrentTime();

        long systime = System.currentTimeMillis();

        if (report.report(null, new ContentBean(newData), event_time, systime)) {
            log.info("[" + Thread.currentThread() + "][" + systime + "] FROM STATEMENT: " + stmt.getText() + " AT "
                    + event_time);
            eval(newData, oldData, event_time);
            setChanged();
            notifyObservers(event_time);
        }

    }

    public void eval(EventBean[] newData, EventBean[] oldData, long currentTime) {
        DStreamUpdate(graph, oldData, maintenance);
        IStreamUpdate(graph, newData);
        now = currentTime;
    }

    private void handleSingleIStream(Graph ii, StreamItem st) {
        log.debug("Handling single IStreamTest [" + st + "]");
        st.addTo(ii);
    }

    private void IStreamUpdate(Graph ii, EventBean[] newData) {
        if (newData != null && newData.length != 0) {
            log.debug("[" + newData.length + "] New Events of type ["
                    + newData[0].getUnderlying().getClass().getSimpleName() + "]");
            for (EventBean e : newData) {
                if (e instanceof MapEventBean) {
                    MapEventBean meb = (MapEventBean) e;
                    if (meb.getProperties() instanceof StreamItem) {
                        handleSingleIStream(ii, (StreamItem) e.getUnderlying());
                    } else {
                        for (int i = 0; i < meb.getProperties().size(); i++) {
                            StreamItem st = (StreamItem) meb.get("stream_" + i);
                            handleSingleIStream(ii, st);
                        }
                    }
                }
            }
        }
    }

    private void handleSingleDStream(Graph ii, StreamItem st) {
        log.debug("Handling single IStreamTest [" + st + "]");
        st.removeFrom(ii);
    }

    private void DStreamUpdate(Graph ii, EventBean[] oldData, Maintenance m) {
        if (Maintenance.NAIVE.equals(m)) {
            ii.clear();
        } else {
            if (oldData != null) {
                log.debug("[" + oldData.length + "] Old Events of type ["
                        + oldData[0].getUnderlying().getClass().getSimpleName() + "]");
                for (EventBean e : oldData) {
                    if (e instanceof MapEventBean) {
                        MapEventBean meb = (MapEventBean) e;
                        if (meb.getProperties() instanceof StreamItem) {
                            handleSingleDStream(ii, (StreamItem) e.getUnderlying());
                        } else {
                            for (int i = 0; i < meb.getProperties().size(); i++) {
                                StreamItem st = (StreamItem) meb.get("stream_" + i);
                                handleSingleDStream(ii, st);
                            }
                        }
                    }
                }
            }
        } //Remove all the data

    }

    @Override
    public synchronized void addObserver(Observer o) {
        super.addObserver(o);
    }

    @Override
    public void materialize(long ts) {
        graph = wa.<Graph>getContent(ts).coalesce();

    }

    @Override
    public Object get() {
        return graph;
    }

}


//    public synchronized void update(EPStatement stmt, Object[][] insertStream, Object[][] removeStream) {
//
//        final Long[] current_timestamp = {(Long) insertStream[0][0]};
//
//        log.debug("[" + Thread.currentThread() + "][" + System.currentTimeMillis() + "] FROM STATEMENT: " + stmt.getText());
//
//        Map<Long, List<StreamItem>[]> grouped_by_time = new HashMap<>();
//
//        Arrays.stream(insertStream).forEach(pairs -> {
//                    Long t = (Long) pairs[0];
//                    if (grouped_by_time.containsKey(t))
//                        grouped_by_time.get(t)[0].addObservable((StreamItem) pairs[1]);
//                    else {
//                        List<StreamItem> events = new ArrayList<>();
//                        events.addObservable((StreamItem) pairs[1]);
//                        List[] value = new List[2];
//                        value[0] = events;
//                        grouped_by_time.put((Long) t, value);
//                    }
//                    current_timestamp[0] = t > current_timestamp[0] ? t : current_timestamp[0];
//                }
//
//
//        );
//
//        if (removeStream != null) {
//            Arrays.stream(removeStream).forEach(pairs -> {
//                        if (grouped_by_time.containsKey(pairs[0]))
//                            grouped_by_time.get(pairs[0])[1].addObservable((StreamItem) pairs[1]);
//                        else {
//                            List<StreamItem> events = new ArrayList<>();
//                            events.addObservable((StreamItem) pairs[1]);
//                            List[] value = new List[2];
//                            value[1] = events;
//                            grouped_by_time.put((Long) pairs[0], value);
//                        }
//                    }
//            );
//        }
//
//        grouped_by_time.forEach((t, ls) -> setTimestamp(ls[0], ls[1], t));
//
//        setChanged();
//        notifyObservers(current_timestamp[0]);
//    }

// public void setTimestamp(List<StreamItem> newData, List<StreamItem> oldData, long currentTime) {
//        Updatable instantaneousItem = getContent().asUpdatable();
//        if (Maintenance.NAIVE.equals(maintenance))
//            instantaneousItem.clear();
//        else oldData.forEach(st -> st.removeFrom(instantaneousItem));
//        newData.forEach(streamItem -> streamItem.addTo(instantaneousItem));
//        setTimestamp(currentTime);
//    }
