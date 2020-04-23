package it.polimi.jasper.engine.execution.formatter.sysout;

import it.polimi.jasper.engine.execution.formatter.ConstructResponseDefaultFormatter;

public class ConstructSysOutDefaultFormatter extends ConstructResponseDefaultFormatter {

    public ConstructSysOutDefaultFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    protected void out(String s) {
        System.out.println(s);
    }
}
