package it.polimi.jasper.sds;

import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.sds.timevarying.TimeVarying;
import it.polimi.yasper.core.secret.time.Time;
import it.polimi.yasper.core.secret.time.TimeFactory;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.rdf.api.IRI;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.compose.MultiUnion;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.InfModelImpl;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.sparql.core.DatasetGraphFactory;

import java.util.*;

/**
 * Created by riccardo on 01/07/2017.
 */
@Log4j
public class JenaSDSGG extends Observable implements SDS<Graph>, Observer {

    private boolean partialWindowsEnabled = false;
    private Time time = TimeFactory.getInstance();
    @Getter
    protected Reasoner reasoner;
    private Dataset ds;

    private List<TimeVarying<Graph>> tvgs = new ArrayList<>();

    protected JenaSDSGG(Graph def) {
        ds = DatasetFactory.wrap(DatasetGraphFactory.create(def));
    }

    @Override
    public Collection<TimeVarying<Graph>> asTimeVaryingEs() {
        return tvgs;
    }

    @Override
    public void add(IRI iri, TimeVarying<Graph> tvg) {
        tvgs.add(tvg);
        Model m;
        if (reasoner != null)
            m = new InfModelImpl((InfGraph) (tvg.get()));
        else
            m = new ModelCom(tvg.get());
        addNamedModel(iri.getIRIString(), m);
    }

    protected void addNamedModel(String iriString, Model m) {
        ds.addNamedModel(iriString, m);
    }

    @Override
    public void add(TimeVarying<Graph> tvg) {
        tvgs.add(tvg);
        ((MultiUnion) getDefaultModel().getGraph())
                .addGraph((tvg.get()));
    }

    @Override
    public void materialize(long ts) {
        tvgs.forEach(g -> g.materialize(time.getAppTime()));
    }


    @Override
    public void update(Observable o, Object arg) {
        materialize((Long) arg);
        setChanged();
        notifyObservers(arg);
    }

    public Model getDefaultModel() {
        return ds.getDefaultModel();
    }

    public Dataset getDS() {
        return ds;
    }
}