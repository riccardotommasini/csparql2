package it.polimi.jasper.jena;

import it.polimi.jasper.secret.content.ContentEventBean;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.operators.s2r.execution.assigner.Assigner;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.secret.report.Report;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

@Log4j
@Getter
public class NamedEsperTimeVaryingGraph extends EsperTimeVaryingGraphImpl {

    private String uri;

    public NamedEsperTimeVaryingGraph(ContentEventBean<Graph, ?, Graph> c, String uri, Maintenance maintenance, Report report, Assigner<Graph, Graph> wo, SDS<Graph> sds) {
        super(c, maintenance, report, wo, sds);
        this.uri = uri;
    }

    @Override
    public String iri() {
        return uri;
    }

    @Override
    public boolean named() {
        return true;
    }


}