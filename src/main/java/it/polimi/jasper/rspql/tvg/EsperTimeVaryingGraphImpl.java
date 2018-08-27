package it.polimi.jasper.rspql.tvg;

import it.polimi.jasper.spe.operators.s2r.EsperWindowAssigner;
import it.polimi.yasper.core.spe.content.Maintenance;
import it.polimi.yasper.core.spe.report.Report;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

@Log4j
public class EsperTimeVaryingGraphImpl extends EsperTimeVaryingGraph {

    public EsperTimeVaryingGraphImpl(Graph content, Maintenance maintenance, Report report, EsperWindowAssigner wo) {
        super(content, maintenance, report, wo);
    }


}