package it.polimi.jasper.engine.rsp.streams;

import com.espertech.esper.client.EPStatement;
import it.polimi.yasper.core.spe.windowing.assigner.WindowAssigner;
import it.polimi.yasper.core.stream.Stream;
import it.polimi.yasper.core.stream.rdf.RegisteredRDFStream;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by riccardo on 10/07/2017.
 */
@Getter
public class RegisteredEPLStream extends RegisteredRDFStream {

    protected Stream stream;
    protected EPStatement e;

    protected final List<WindowAssigner> assigners = new ArrayList<>();

    public RegisteredEPLStream(String uri, Stream s, EPStatement epl) {
        super(uri);
        this.stream = s;
    }

    @Override
    public void addWindowAssiger(WindowAssigner windowAssigner) {
        assigners.add(windowAssigner);
    }

}
