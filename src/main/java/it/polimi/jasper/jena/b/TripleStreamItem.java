package it.polimi.jasper.jena.b;

import it.polimi.jasper.streams.items.RDFStreamItem;
import org.apache.jena.graph.Triple;

//TODO wrap rid of  JenaGraph
public class TripleStreamItem extends RDFStreamItem<Triple> {

    private static final long serialVersionUID = 1L;

    public TripleStreamItem(long appTimestamp1, Triple content1, String stream_uri) {
        super(appTimestamp1, content1, stream_uri);
    }

    @Override
    public String toString() {
        return "GraphStreamItem {" + "appTimestamp='" + getAppTimestamp() + '\'' + ", sysTimestamp='" + getSysTimestamp()
                + '\'' + ", content='" + getTypedContent() + '\'' + ", stream_uri='" + getStream_uri() + '\'' + '}';
    }

    @Override
    public Triple addTo(Triple abox) {
        this.put(content, abox);
        return getTypedContent();
    }

    @Override
    public Triple removeFrom(Triple abox) {
        Triple t = getTypedContent();
        if (getTypedContent().equals(abox))
            this.put(content, null);
        return t;
    }

    @Override
    public String getStreamURI() {
        return getStream_uri();
    }
}
