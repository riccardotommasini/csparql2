package it.polimi.jasper.spe.esper;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.time.CurrentTimeEvent;
import it.polimi.yasper.core.spe.time.ET;
import it.polimi.yasper.core.spe.time.Time;
import it.polimi.yasper.core.spe.time.TimeFactory;
import it.polimi.yasper.core.spe.time.TimeInstant;

public class EsperTime implements Time {

    private final EPRuntime cepRT;

    public EsperTime(EPRuntime cepRT) {
        this.cepRT = cepRT;
    }

    @Override
    public long getAppTime() {
        return cepRT.getCurrentTime();
    }

    @Override
    public void setAppTime(long now) {
        cepRT.sendEvent(new CurrentTimeEvent(now));
    }

    @Override
    public ET getEvaluationTimeInstants() {
        return TimeFactory.getEvaluationTimeInstants();
    }

    @Override
    public void addEvaluationTimeInstants(TimeInstant i) {
        TimeFactory.getEvaluationTimeInstants().add(i);
    }
}
