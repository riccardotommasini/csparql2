package it.polimi.jasper.formatter;


import it.polimi.jasper.formatter.sysout.ConstructSysOutDefaultFormatter;
import it.polimi.jasper.formatter.sysout.SelectSysOutDefaultFormatter;
import it.polimi.yasper.core.format.QueryResultFormatter;

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
