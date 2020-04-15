package it.polimi.seraph.sds;

import it.polimi.seraph.s2r.EsperWindowAssignerPGraph;
import it.polimi.seraph.streans.PGraph;
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