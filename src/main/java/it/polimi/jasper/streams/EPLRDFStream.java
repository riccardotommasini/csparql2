package it.polimi.jasper.streams;

import com.espertech.esper.client.EPStatement;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import it.polimi.yasper.core.stream.web.WebStream;
import lombok.Getter;
import org.apache.jena.graph.Graph;

/**
 * Created by riccardo on 10/07/2017.
 */
@Getter
public class EPLRDFStream extends DataStreamImpl<Graph> {

    protected WebStream stream;
    protected EPStatement e;

    public EPLRDFStream(String uri, WebStream s, EPStatement epl) {
        super(uri);
        this.stream = s;
        this.e = epl;
    }

}
