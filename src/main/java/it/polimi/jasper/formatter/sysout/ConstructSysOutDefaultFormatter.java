package it.polimi.jasper.formatter.sysout;

import it.polimi.jasper.formatter.ConstructResponseDefaultFormatter;

public class ConstructSysOutDefaultFormatter extends ConstructResponseDefaultFormatter {

    public ConstructSysOutDefaultFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    protected void out(String s) {
        System.out.println(s);
    }
}
