package it.polimi.jasper.engine.spe.windowing;

import it.polimi.jasper.engine.spe.esper.EPLFactory;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.quering.rspql.window.WindowNode;
import it.polimi.yasper.core.spe.report.Report;
import it.polimi.yasper.core.spe.report.ReportGrain;
import it.polimi.yasper.core.spe.scope.Tick;
import it.polimi.yasper.core.spe.windowing.assigner.WindowAssigner;
import it.polimi.yasper.core.spe.windowing.operator.WindowOperator;
import it.polimi.yasper.core.stream.RegisteredStream;
import lombok.RequiredArgsConstructor;
import org.apache.jena.reasoner.Reasoner;

@RequiredArgsConstructor
public class EsperWindowOperator implements WindowOperator {

    private final Tick tick;
    private final Report report;
    private final Reasoner reasoner;
    private final Boolean eventtime;
    private final ReportGrain reportGrain;
    private final Maintenance maintenance;

    private final WindowNode wo;


    @Override
    public String getName() {
        return wo.getName();
    }

    @Override
    public WindowAssigner apply(RegisteredStream s) {
        EsperWindowAssigner windowAssigner = EPLFactory.getWindowAssigner(tick, maintenance, report, eventtime, s.getURI(), wo.getStep(), wo.getRange(), wo.getUnitStep(), wo.getUnitRange(), wo.getType());
        windowAssigner.setReasoner(reasoner);
        windowAssigner.setMaintenance(maintenance);
        s.addWindowAssiger(windowAssigner);
        return windowAssigner;
    }

    @Override
    public boolean isNamed() {
        return wo.isNamed();
    }
}
