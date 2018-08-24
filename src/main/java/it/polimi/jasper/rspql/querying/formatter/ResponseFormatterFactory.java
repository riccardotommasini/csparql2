package it.polimi.jasper.rspql.querying.formatter;


import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;

/**
 * Created by riccardo on 10/07/2017.
 */
public class ResponseFormatterFactory {

    public static QueryResultFormatter getSelectResponseSysOutFormatter(String format, boolean distinct) {
        return new SelectResponseSysOutFormatter(format, distinct);
    }

    public static QueryResultFormatter getConstructResponseSysOutFormatter(String format, boolean distinct) {
        return new ConstructResponseSysOutFormatter(format, distinct);
    }

    public static QueryResultFormatter getGenericResponseSysOutFormatter(String format, boolean distinct) {
        return new GenericResponseSysOutFormatter(format, distinct, System.out);
    }
}
