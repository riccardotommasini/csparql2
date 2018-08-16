package it.polimi.jasper.engine.spe.report;

import it.polimi.yasper.core.spe.content.Content;
import it.polimi.yasper.core.spe.report.strategies.ReportingStrategy;
import it.polimi.yasper.core.spe.windowing.definition.Window;

public class EsperCCReportStrategy implements ReportingStrategy {

    @Override
    public boolean match(Window w, Content c, long tapp, long tsys) {
        return true;
    }

}