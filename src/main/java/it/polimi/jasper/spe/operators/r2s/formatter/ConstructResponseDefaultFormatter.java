package it.polimi.jasper.spe.operators.r2s.formatter;

import it.polimi.jasper.spe.operators.r2s.results.ConstructResponse;
import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;
import lombok.extern.log4j.Log4j;

import java.io.ByteArrayOutputStream;
import java.util.Observable;

/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j
public abstract class ConstructResponseDefaultFormatter extends QueryResultFormatter {

    long last_result = -1L;

    public ConstructResponseDefaultFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    public void update(Observable o, Object arg) {
        ConstructResponse sr = (ConstructResponse) arg;
        this.format(sr);
    }

    public void format(ConstructResponse sr) {
        long cep_timestamp = sr.getCep_timestamp();
        if (cep_timestamp != last_result && distinct) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            last_result = cep_timestamp;
            sr.getResults().write(outputStream, format);
            log.debug("[" + System.currentTimeMillis() + "] Result at [" + last_result + "]");
            out(new String(outputStream.toByteArray()));
        }
    }

    protected abstract void out(String s);
}
