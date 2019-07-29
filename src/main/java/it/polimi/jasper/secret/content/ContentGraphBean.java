package it.polimi.jasper.secret.content;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.event.map.MapEventBean;
import it.polimi.jasper.streams.items.GraphStreamItem;
import it.polimi.jasper.streams.items.StreamItem;
import it.polimi.yasper.core.secret.content.Content;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;

import java.util.ArrayList;
import java.util.List;

@Log4j
public class ContentGraphBean implements Content<Graph> {

    protected List<Graph> elements;
    protected Graph graph;

    @Setter
    private long last_timestamp_changed;

    public ContentGraphBean(Graph graph) {
        this.graph = graph;
        this.elements = new ArrayList<>();
    }

    public void eval(EventBean[] newData, EventBean[] oldData) {
        DStreamUpdate(oldData);
        IStreamUpdate(newData);
    }

    private void handleSingleIStream(GraphStreamItem st) {
       // log.debug("Handling single IStreamTest [" + st + "]");
        elements.add(st.getTypedContent());
    }

    private void IStreamUpdate(EventBean[] newData) {
        if (newData != null && newData.length != 0) {
            log.debug("[" + newData.length + "] New Events of type ["
                    + newData[0].getUnderlying().getClass().getSimpleName() + "]");
            for (EventBean e : newData) {
                if (e instanceof MapEventBean) {
                    MapEventBean meb = (MapEventBean) e;
                    if (meb.getProperties() instanceof GraphStreamItem) {
                        handleSingleIStream((GraphStreamItem) e.getUnderlying());
                    } else {
                        for (int i = 0; i < meb.getProperties().size(); i++) {
                            GraphStreamItem st = (GraphStreamItem) meb.get("stream_" + i);
                            handleSingleIStream(st);
                        }
                    }
                }
            }
        }
    }

    protected void DStreamUpdate(EventBean[] oldData) {
        elements.clear();
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void add(Graph e) {
        elements.add(e);
    }


    public void add(EventBean e) {
        if (e instanceof MapEventBean) {
            MapEventBean meb = (MapEventBean) e;
            if (meb.getUnderlying() instanceof GraphStreamItem) {
                elements.add((Graph) ((StreamItem) meb.getUnderlying()).getTypedContent());
            } else {
                for (int i = 0; i < meb.getProperties().size(); i++) {
                    GraphStreamItem st = (GraphStreamItem) meb.get("stream_" + i);
                    elements.add(st.getTypedContent());
                }
            }
        }
    }

    @Override
    public Long getTimeStampLastUpdate() {
        return last_timestamp_changed;
    }

    @Override
    public Graph coalesce() {
        graph.clear();
        elements.forEach(ig -> GraphUtil.addInto(this.graph, ig));
        //        elements.stream().flatMap(ig->GraphUtil.findAll(ig).toList().stream()).forEach(this.graph::add);

        return this.graph;
    }

    @Override
    public String toString() {
        return elements.toString();
    }

    public EventBean[] asArray() {
        return elements.toArray(new EventBean[size()]);
    }

    public void update(EventBean[] newData, EventBean[] oldData, long event_time) {
        eval(newData, oldData);
        setLast_timestamp_changed(event_time);
    }

    public void replace(Graph coalesce) {
        this.graph.clear();
        GraphUtil.addInto(graph, coalesce);
    }
}
