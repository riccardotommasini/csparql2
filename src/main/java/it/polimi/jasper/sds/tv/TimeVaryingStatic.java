package it.polimi.jasper.sds.tv;

import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.sds.timevarying.TimeVarying;
import it.polimi.yasper.core.secret.content.Content;
import lombok.Getter;
import lombok.extern.log4j.Log4j;

/**
 * Created by riccardo on 05/07/2017.
 */
@Log4j
@Getter
public class TimeVaryingStatic<O> implements TimeVarying<O> {

    private final SDS<O> sds;
    protected long now;
    protected Content<O, O> content;
    private String iri;

    public TimeVaryingStatic(SDS<O> sds, O content) {
        this(sds, content, null);
    }


    public TimeVaryingStatic(SDS<O> sds, O content, String iri) {
        this.sds = sds;
        this.iri = iri;
        this.content = new Content<O, O>() {
            @Override
            public int size() {
                return 1;
            }

            @Override
            public void add(O e) {

            }

            @Override
            public Long getTimeStampLastUpdate() {
                return now;
            }

            @Override
            public O coalesce() {
                return content;
            }
        };
    }


    @Override
    public void materialize(long ts) {
        this.content.coalesce();
    }

    @Override
    public O get() {
        return content.coalesce();
    }

    @Override
    public String iri() {
        return iri;
    }

    @Override
    public boolean named() {
        return iri != null;
    }


}
