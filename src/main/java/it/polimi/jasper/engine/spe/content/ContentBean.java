package it.polimi.jasper.engine.spe.content;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.event.map.MapEventBean;
import it.polimi.jasper.engine.rsp.streams.items.StreamItem;
import it.polimi.yasper.core.spe.content.Content;
import it.polimi.yasper.core.stream.StreamElement;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.ModelFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Log4j
public class ContentBean implements Content<Graph> {

    private List<EventBean> elements;

    @Setter
    private long last_timestamp_changed;

    public ContentBean(EventBean[] arr) {
        this.elements = Arrays.asList(arr);
    }

    public ContentBean() {
        this.elements = new ArrayList<>();
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void add(StreamElement e) {
        add((EventBean) e.getContent());
        System.out.println();
        this.last_timestamp_changed = e.getTimestamp();
    }

    public void add(EventBean e) {
        elements.add(e);
    }

    @Override
    public Long getTimeStampLastUpdate() {
        return last_timestamp_changed;
    }

    @Override
    public Graph coalesce() {
        Graph g = ModelFactory.createDefaultModel().getGraph();
        RStreamUpdate(g, elements);
        return g;
    }

    private void handleSingleRStream(Graph ii, StreamItem st) {
        log.debug("Handling single IStreamTest [" + st + "]");
        st.addTo(ii);
    }

    private void RStreamUpdate(Graph ii, List<EventBean> newData) {
        if (newData != null && newData.size() != 0) {
            log.debug("[" + newData.size() + "] New Events of type ["
                    + newData.get(0).getUnderlying().getClass().getSimpleName() + "]");
            newData.forEach(eb -> {
                if (eb instanceof MapEventBean) {
                    MapEventBean meb = (MapEventBean) eb;
                    if (meb.getProperties() instanceof StreamItem) {
                        handleSingleRStream(ii, (StreamItem) eb.getUnderlying());
                    } else {
                        for (int i = 0; i < meb.getProperties().size(); i++) {
                            StreamItem st = (StreamItem) meb.get("stream_" + i);
                            handleSingleRStream(ii, st);
                        }
                    }
                }
            });
        }
    }

    @Override
    public String toString() {
        return elements.toString();
    }

    public EventBean[] asArray() {
        return elements.toArray(new EventBean[size()]);
    }
}
