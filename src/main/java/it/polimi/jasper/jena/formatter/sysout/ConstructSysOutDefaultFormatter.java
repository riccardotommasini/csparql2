package it.polimi.jasper.jena.formatter.sysout;

public class ConstructSysOutDefaultFormatter extends ConstructResponseDefaultFormatter {

    public ConstructSysOutDefaultFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    protected void out(String s) {
        System.out.println(s);
    }
}
