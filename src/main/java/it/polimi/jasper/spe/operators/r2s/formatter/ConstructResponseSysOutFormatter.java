package it.polimi.jasper.spe.operators.r2s.formatter;

import it.polimi.jasper.spe.operators.r2s.results.ConstructResponse;
import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j;

import java.util.Observable;

/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j
public class ConstructResponseSysOutFormatter extends QueryResultFormatter {

    long last_result = -1L;

    @NonNull
    @Getter
    boolean distinct;

    public ConstructResponseSysOutFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    public void update(Observable o, Object arg) {
        ConstructResponse sr = (ConstructResponse) arg;
        long cep_timestamp = sr.getCep_timestamp();
        if (cep_timestamp != last_result && distinct) {
            last_result = cep_timestamp;
            sr.getResults().write(System.out, format);
            log.info("[" + System.currentTimeMillis() + "] Result at [" + last_result + "]");
        }
    }
}
