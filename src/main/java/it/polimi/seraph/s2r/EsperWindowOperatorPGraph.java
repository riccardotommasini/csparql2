package it.polimi.seraph.s2r;

import it.polimi.jasper.operators.s2r.EPLFactory;
import it.polimi.seraph.streans.PGraph;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.enums.ReportGrain;
import it.polimi.yasper.core.enums.Tick;
import it.polimi.yasper.core.operators.s2r.StreamToRelationOperator;
import it.polimi.yasper.core.operators.s2r.execution.assigner.Assigner;
import it.polimi.yasper.core.operators.s2r.syntax.WindowNode;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.timevarying.TimeVarying;
import it.polimi.yasper.core.secret.report.Report;
import it.polimi.yasper.core.secret.time.Time;
import it.polimi.yasper.core.stream.data.WebDataStream;
import lombok.RequiredArgsConstructor;
import org.apache.jena.reasoner.Reasoner;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class EsperWindowOperatorPGraph implements StreamToRelationOperator<PGraph, PGraph> {

    private final Tick tick;
    private final Report report;
    private final Reasoner reasoner;
    private final Boolean eventtime;
    private final ReportGrain reportGrain;
    private final Maintenance maintenance;
    private final Time time;
    private final WindowNode wo;
    private final ContinuousQueryExecution context;

    private final List<Assigner> assigners = new ArrayList();

    @Override
    public String iri() {
        return wo.iri();
    }

    @Override
    public boolean named() {
        return wo.named();
    }

    @Override
    public TimeVarying<PGraph> apply(WebDataStream<PGraph> s) {
        EsperWindowAssignerPGraph windowAssigner = EPLFactory.getWindowAssignerPGraph(tick, maintenance, report, eventtime, s.getURI(), wo.getStep(), wo.getRange(), wo.getUnitStep(), wo.getUnitRange(), wo.getType(), time);
        windowAssigner.setReasoner(reasoner);
        windowAssigner.setMaintenance(maintenance);
        s.addConsumer(windowAssigner);
        this.assigners.add(windowAssigner);
        return windowAssigner.set(context);
    }

}
