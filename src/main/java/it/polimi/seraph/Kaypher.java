package it.polimi.seraph;

import it.polimi.jasper.engine.EsperRSPEngine;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.engine.features.QueryObserverRegistrationFeature;
import it.polimi.yasper.core.engine.features.QueryRegistrationFeature;
import it.polimi.yasper.core.engine.features.QueryStringRegistrationFeature;
import it.polimi.yasper.core.format.QueryResultFormatter;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;

public class Kaypher extends EsperRSPEngine2 implements QueryObserverRegistrationFeature, QueryRegistrationFeature<Seraph>, QueryStringRegistrationFeature {

    public Kaypher(long t0, EngineConfiguration configuration) {
        super(t0, configuration);
    }

    @Override
    public void register(ContinuousQuery continuousQuery, QueryResultFormatter queryResultFormatter) {

    }

    @Override
    public ContinuousQueryExecution register(Seraph seraph) {
        return null;
    }

    @Override
    public ContinuousQueryExecution register(Seraph seraph, SDSConfiguration sdsConfiguration) {
        return null;
    }

    @Override
    public ContinuousQueryExecution register(String s) {
        return null;
    }

    @Override
    public ContinuousQueryExecution register(String s, SDSConfiguration sdsConfiguration) {
        return null;
    }
}