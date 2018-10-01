package it.polimi.jasper.spe.operators.r2s.formatter.sysout;

import it.polimi.jasper.spe.operators.r2s.formatter.ConstructResponseDefaultFormatter;

public class ConstructSysOutDefaultFormatter extends ConstructResponseDefaultFormatter {

    public ConstructSysOutDefaultFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    protected void out(String s) {
        System.out.println(s);
    }
}
