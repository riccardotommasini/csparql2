package it.polimi.jasper.spe.operators.r2s.formatter;


import it.polimi.jasper.spe.operators.r2s.formatter.sysout.ConstructSysOutDefaultFormatter;
import it.polimi.jasper.spe.operators.r2s.formatter.sysout.SelectSysOutDefaultFormatter;
import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;

/**
 * Created by riccardo on 10/07/2017.
 */
public class ResponseFormatterFactory {

    public static QueryResultFormatter getSelectResponseSysOutFormatter(String format, boolean distinct) {
        return new SelectSysOutDefaultFormatter(format, distinct);
    }

    public static QueryResultFormatter getConstructResponseSysOutFormatter(String format, boolean distinct) {
        return new ConstructSysOutDefaultFormatter(format, distinct);
    }

}
