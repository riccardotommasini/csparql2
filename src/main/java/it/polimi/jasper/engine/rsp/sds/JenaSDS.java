package it.polimi.jasper.engine.rsp.sds;

import it.polimi.jasper.engine.spe.windowing.EsperTimeVaryingGraphImpl;
import it.polimi.yasper.core.quering.rspql.sds.SDS;
import it.polimi.yasper.core.quering.rspql.tvg.TimeVarying;
import it.polimi.yasper.core.spe.time.Time;
import it.polimi.yasper.core.spe.time.TimeFactory;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.rdf.api.IRI;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.compose.MultiUnion;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.InfModelImpl;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.DatasetImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by riccardo on 01/07/2017.
 */
@Log4j
public class JenaSDS extends DatasetImpl implements SDS {

    private boolean partialWindowsEnabled = false;
    private Time time = TimeFactory.getInstance();
    @Getter
    protected Reasoner reasoner;

    private List<TimeVarying> tvgs = new ArrayList<>();

    protected JenaSDS(Graph def) {
        super(DatasetGraphFactory.create(def));
    }

    @Override
    public void add(IRI iri, TimeVarying tvg) {
        tvgs.add(tvg);
        if (tvg instanceof EsperTimeVaryingGraphImpl) {
            Model m;
            if (reasoner != null)
                m = new InfModelImpl((InfGraph) ((EsperTimeVaryingGraphImpl) tvg).getGraph());
            else
                m = new ModelCom(((EsperTimeVaryingGraphImpl) tvg).getGraph());
            addNamedModel(iri.getIRIString(), m);
        }
    }

    @Override
    public void add(TimeVarying tvg) {
        tvgs.add(tvg);
        if (tvg instanceof EsperTimeVaryingGraphImpl) {
            ((MultiUnion) getDefaultModel().getGraph())
                    .addGraph((((EsperTimeVaryingGraphImpl) tvg).getGraph()));
        }
    }

    @Override
    public void materialize(long ts) {
        if (partialWindowsEnabled) {
            tvgs.forEach(g -> g.materialize(time.getAppTime()));
        } //I don't need to re add them to the sds
    }


}