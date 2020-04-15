package it.polimi.seraph;

import it.polimi.seraph.streans.EPLPGraphStream;
import it.polimi.yasper.core.stream.web.WebStreamImpl;

public class PGraphStream extends WebStreamImpl implements Runnable {
    private EPLPGraphStream stream;

    public PGraphStream(String stream_uri) {
        super(stream_uri);
    }

    @Override
    public void run() {
        //todo stream.put();
    }

    public void setWritable(EPLPGraphStream register) {
        this.stream = register;
    }
}
