package it.polimi.seraph.streans;

import com.espertech.esper.client.EPStatement;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import it.polimi.yasper.core.stream.web.WebStream;
import lombok.Getter;

/**
 * Created by riccardo on 10/07/2017.
 */
@Getter
public class EPLPGraphStream extends DataStreamImpl<PGraph> {

    protected WebStream stream;
    protected EPStatement e;

    public EPLPGraphStream(String uri, WebStream s, EPStatement epl) {
        super(uri);
        this.stream = s;
        this.e = epl;
    }

}
