package it.polimi.jasper.spe.operators.r2s.formatter.sysout;

import it.polimi.jasper.spe.operators.r2s.formatter.sysout.ConstructSysOutDefaultFormatter;
import it.polimi.jasper.spe.operators.r2s.formatter.sysout.SelectSysOutDefaultFormatter;
import it.polimi.jasper.spe.operators.r2s.results.ConstructResponse;
import it.polimi.jasper.spe.operators.r2s.results.SelectResponse;
import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;
import lombok.extern.log4j.Log4j;

import java.util.Observable;

/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j
public class GenericResponseSysOutFormatter extends QueryResultFormatter {

    private final SelectSysOutDefaultFormatter sf;
    private final ConstructSysOutDefaultFormatter cf;
    long last_result = -1L;

    public GenericResponseSysOutFormatter(String format, boolean distinct) {
        super(format, distinct);

        this.cf = new ConstructSysOutDefaultFormatter(format, distinct);
        this.sf = new SelectSysOutDefaultFormatter(format, distinct);

    }

    @Override
    public void update(Observable o, Object arg) {
        if (arg instanceof SelectResponse) {
            sf.format((SelectResponse) arg);
        } else if (arg instanceof ConstructResponse) {
            cf.format((ConstructResponse) arg);
        }
    }
}
