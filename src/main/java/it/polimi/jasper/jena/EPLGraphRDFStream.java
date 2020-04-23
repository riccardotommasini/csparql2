package it.polimi.jasper.jena;

import com.espertech.esper.client.EPStatement;
import it.polimi.jasper.streams.EPLStream;
import it.polimi.yasper.core.stream.web.WebStream;
import lombok.Getter;
import org.apache.jena.graph.Graph;

/**
 * Created by riccardo on 10/07/2017.
 */
@Getter
public class EPLGraphRDFStream extends EPLStream<Graph> {

    public EPLGraphRDFStream(String uri, WebStream s, EPStatement epl) {
        super(uri, s, epl);
    }
}
