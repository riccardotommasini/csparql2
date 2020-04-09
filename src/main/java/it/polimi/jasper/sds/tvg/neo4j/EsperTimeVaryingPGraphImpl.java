package it.polimi.jasper.sds.tvg.neo4j;

import it.polimi.jasper.operators.s2r.EsperWindowAssigner;
import it.polimi.jasper.operators.s2r.neo4j.EsperWindowAssignerPGraph;
import it.polimi.jasper.streams.neo4j.PGraph;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.secret.report.Report;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

@Log4j
public class EsperTimeVaryingPGraphImpl extends EsperTimeVaryingPGraph {

    public EsperTimeVaryingPGraphImpl(PGraph content, Maintenance maintenance, Report report, EsperWindowAssignerPGraph wo) {
        super(content, maintenance, report, wo);
    }


}