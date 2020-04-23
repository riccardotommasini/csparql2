package it.polimi.jasper.jena.b;

import com.espertech.esper.client.EPStatement;
import it.polimi.jasper.streams.EPLStream;
import it.polimi.yasper.core.stream.web.WebStream;
import lombok.Getter;
import org.apache.jena.graph.Triple;

/**
 * Created by riccardo on 10/07/2017.
 */
@Getter
public class EPLGraphTripleStream extends EPLStream<Triple> {

    public EPLGraphTripleStream(String uri, WebStream s, EPStatement epl) {
        super(uri, s, epl);
    }
}
