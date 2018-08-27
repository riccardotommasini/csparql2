package it.polimi.jasper.spe.operators.r2s.formatter;

import it.polimi.jasper.spe.operators.r2s.results.SelectResponse;
import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;
import org.apache.jena.query.ResultSetFormatter;

import java.util.Observable;

/**
 * Created by riccardo on 03/07/2017.
 */

public class SelectResponseSysOutFormatter extends QueryResultFormatter {

    long last_result = -1L;

    public SelectResponseSysOutFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    public void update(Observable o, Object arg) {
        SelectResponse sr = (SelectResponse) arg;
        if (sr.getCep_timestamp() != last_result && distinct) {
            System.out.println("[" + System.currentTimeMillis() + "] Result at [" + last_result + "]");
            ResultSetFormatter.out(System.out, sr.getResults());
            last_result = sr.getCep_timestamp();
        }

    }
}
