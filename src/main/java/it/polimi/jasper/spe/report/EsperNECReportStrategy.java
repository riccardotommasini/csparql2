package it.polimi.jasper.spe.report;

import it.polimi.yasper.core.spe.content.Content;
import it.polimi.yasper.core.spe.operators.s2r.execution.instance.Window;
import it.polimi.yasper.core.spe.report.strategies.ReportingStrategy;

public class EsperNECReportStrategy implements ReportingStrategy {

    @Override
    public boolean match(Window w, Content c, long tapp, long tsys) {
        return c.size() != 0;
    }

}
