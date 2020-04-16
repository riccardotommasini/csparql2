package it.polimi.jasper.secret.content;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.event.map.MapEventBean;
import it.polimi.jasper.streams.items.GraphStreamItem;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

@Log4j
public class IncrementalContentGraphBean extends JenaGraphContent {

    public IncrementalContentGraphBean(Graph graph) {
        super(graph);

    }

    @Override
    protected void DStreamUpdate(EventBean[] oldData) {
        if (oldData != null) {
            log.debug("[" + oldData.length + "] Old Events of type ["
                    + oldData[0].getUnderlying().getClass().getSimpleName() + "]");
            for (EventBean e : oldData) {
                if (e instanceof MapEventBean) {
                    MapEventBean meb = (MapEventBean) e;
                    if (meb.getProperties() instanceof GraphStreamItem) {
                        handleSingleDStream((GraphStreamItem) e.getUnderlying());
                    } else {
                        for (int i = 0; i < meb.getProperties().size(); i++) {
                            GraphStreamItem st = (GraphStreamItem) meb.get("stream_" + i);
                            handleSingleDStream(st);
                        }
                    }
                }
            }
        }
    }

    protected void handleSingleDStream(GraphStreamItem st) {
        log.debug("Handling single IStreamTest [" + st + "]");
        elements.remove(st.getTypedContent());
    }
}
