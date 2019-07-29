package it.polimi.jasper.sds.tvg;

import it.polimi.jasper.operators.s2r.EsperWindowAssigner;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.secret.report.Report;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

@Log4j
@Getter
public class NamedEsperTimeVaryingGraph extends EsperTimeVaryingGraphImpl {

    private String uri;

    public NamedEsperTimeVaryingGraph(String uri, Graph content, Maintenance maintenance, Report report, EsperWindowAssigner wo) {
        super(content, maintenance, report, wo);
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