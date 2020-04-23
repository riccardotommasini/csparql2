package it.polimi.jasper.sds;

import it.polimi.jasper.secret.content.BindingSet;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.sds.timevarying.TimeVarying;
import it.polimi.yasper.core.secret.time.Time;
import it.polimi.yasper.core.secret.time.TimeFactory;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.rdf.api.IRI;

import java.util.*;

/**
 * Created by riccardo on 01/07/2017.
 */
@Log4j
public class JenaSDSBB extends Observable implements SDS<BindingSet>, Observer {

    private Time time = TimeFactory.getInstance();

    @Getter
    private List<TimeVarying<BindingSet>> tvgs = new ArrayList<>();
    @Getter
    private Map<IRI, TimeVarying<BindingSet>> named_tvg = new HashMap<>();

    @Override
    public Collection<TimeVarying<BindingSet>> asTimeVaryingEs() {
        return tvgs;
    }

    @Override
    public void add(IRI iri, TimeVarying<BindingSet> timeVarying) {
        named_tvg.put(iri, timeVarying);
    }

    @Override
    public void add(TimeVarying<BindingSet> timeVarying) {
        tvgs.add(timeVarying);
    }

    @Override
    public void materialize(long ts) {
        named_tvg.forEach((iri, g) -> g.materialize(time.getAppTime()));
        tvgs.forEach(g -> g.materialize(time.getAppTime())
        );
    }


    @Override
    public void update(Observable o, Object arg) {
        materialize((Long) arg);
        setChanged();
        notifyObservers(arg);
    }

}