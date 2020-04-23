package it.polimi.jasper.engine.execution.formatter.sysout;

import it.polimi.jasper.engine.execution.formatter.SelectResponseDefaultFormatter;

public class SelectSysOutDefaultFormatter extends SelectResponseDefaultFormatter {

    public SelectSysOutDefaultFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    protected void out(String s) {
        System.out.println(s);
    }
}
