package it.polimi.jasper.spe.operators.s2r;

import it.polimi.jasper.spe.esper.EPLFactory;
import it.polimi.yasper.core.spe.content.Maintenance;
import it.polimi.yasper.core.spe.operators.s2r.WindowOperator;
import it.polimi.yasper.core.spe.operators.s2r.execution.assigner.WindowAssigner;
import it.polimi.yasper.core.spe.operators.s2r.syntax.WindowNode;
import it.polimi.yasper.core.spe.report.Report;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.tick.Tick;
import it.polimi.yasper.core.stream.RegisteredStream;
import lombok.RequiredArgsConstructor;
import org.apache.jena.graph.Graph;
import org.apache.jena.reasoner.Reasoner;

@RequiredArgsConstructor
public class EsperWindowOperator implements WindowOperator<Graph, Graph> {

    private final Tick tick;
    private final Report report;
    private final Reasoner reasoner;
    private final Boolean eventtime;
    private final ReportGrain reportGrain;
    private final Maintenance maintenance;

    private final WindowNode wo;

    @Override
    public String iri() {
        return wo.iri();
    }

    @Override
    public boolean named() {
        return wo.named();
    }

    @Override
    public WindowAssigner<Graph, Graph> apply(RegisteredStream<Graph> s) {
        EsperWindowAssigner windowAssigner = EPLFactory.getWindowAssigner(tick, maintenance, report, eventtime, wo.iri(), s.getURI(), wo.getStep(), wo.getRange(), wo.getUnitStep(), wo.getUnitRange(), wo.getType());
        windowAssigner.setReasoner(reasoner);
        windowAssigner.setMaintenance(maintenance);
        s.addWindowAssiger(windowAssigner);
        return windowAssigner;
    }

}
