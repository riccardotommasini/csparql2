package it.polimi.jasper.sds.tvg.neo4j;

import it.polimi.jasper.operators.s2r.neo4j.EsperWindowAssignerPGraph;
import it.polimi.jasper.streams.neo4j.PGraph;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.secret.report.Report;
import lombok.Getter;
import lombok.extern.log4j.Log4j;

@Log4j
@Getter
public class NamedEsperTimeVaryingPGraph extends EsperTimeVaryingPGraphImpl {

    private String uri;

    public NamedEsperTimeVaryingPGraph(String uri, PGraph content, Maintenance maintenance, Report report, EsperWindowAssignerPGraph wo) {
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