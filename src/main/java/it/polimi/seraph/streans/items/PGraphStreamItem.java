package it.polimi.seraph.streans.items;

import it.polimi.jasper.streams.items.RDFStreamItem;
import it.polimi.seraph.streans.PGraph;

//TODO wrap rid of  JenaGraph
public class PGraphStreamItem extends RDFStreamItem<PGraph> {

    private static final long serialVersionUID = 1L;

    public PGraphStreamItem(long appTimestamp1, PGraph content1, String stream_uri) {
        super(appTimestamp1, content1, stream_uri);
    }

    public PGraph addTo(PGraph abox) {
        PGraph typedContent = this.getTypedContent();
        typedContent.getNodes().addAll(abox.getNodes());
        typedContent.getEdges().addAll(abox.getEdges());
        return abox;
    }

    public PGraph removeFrom(PGraph abox) {
        PGraph typedContent = this.getTypedContent();
        typedContent.getEdges().removeAll(abox.getEdges());
        typedContent.getNodes().removeAll(abox.getNodes());
        return abox;
    }

    @Override
    public String toString() {
        return "GraphStreamItem {" + "appTimestamp='" + getAppTimestamp() + '\'' + ", sysTimestamp='" + getSysTimestamp()
                + '\'' + ", content='" + getTypedContent() + '\'' + ", stream_uri='" + getStream_uri() + '\'' + '}';
    }

    @Override
    public String getStreamURI() {
        return getStream_uri();
    }
}
