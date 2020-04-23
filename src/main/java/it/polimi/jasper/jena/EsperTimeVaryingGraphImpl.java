package it.polimi.jasper.jena;

import it.polimi.jasper.sds.tv.EsperTimeVaryingGeneric;
import it.polimi.jasper.secret.content.ContentEventBean;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.operators.s2r.execution.assigner.Assigner;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.secret.report.Report;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;

@Log4j
public class EsperTimeVaryingGraphImpl extends EsperTimeVaryingGeneric<Graph, Graph> {

    public EsperTimeVaryingGraphImpl(ContentEventBean<Graph, ?, Graph> c, Maintenance maintenance, Report report, Assigner<Graph, Graph> wo, SDS<Graph> sds) {
        super(c, maintenance, report, wo, sds);
    }


}