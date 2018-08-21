package it.polimi.jasper.engine.rsp.streams;

import com.espertech.esper.client.EPStatement;
import it.polimi.yasper.core.stream.Stream;
import it.polimi.yasper.core.stream.rdf.RegisteredRDFStream;
import lombok.Getter;
import org.apache.jena.graph.Graph;

/**
 * Created by riccardo on 10/07/2017.
 */
@Getter
public class RegisteredEPLStream extends RegisteredRDFStream<Graph> {

    protected Stream stream;
    protected EPStatement e;

    public RegisteredEPLStream(String uri, Stream s, EPStatement epl) {
        super(uri);
        this.stream = s;
        this.e = epl;
    }

}
