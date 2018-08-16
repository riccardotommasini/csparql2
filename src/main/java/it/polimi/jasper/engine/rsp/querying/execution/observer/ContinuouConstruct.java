package it.polimi.jasper.engine.rsp.querying.execution.observer;

import it.polimi.jasper.engine.rsp.querying.response.ConstructResponse;
import it.polimi.yasper.core.quering.operators.r2s.RelationToStreamOperator;
import it.polimi.yasper.core.quering.querying.ContinuousQuery;
import it.polimi.yasper.core.quering.response.InstantaneousResponse;
import it.polimi.yasper.core.quering.rspql.sds.SDS;
import org.apache.jena.query.QueryExecutionFactory;

/**
 * Created by riccardo on 03/07/2017.
 */
public class ContinuouConstruct extends JenaContinuousQueryExecution {

    public ContinuouConstruct(ContinuousQuery query, SDS sds, RelationToStreamOperator s2r) {
        super(query, sds, s2r);
    }

    @Override
    public InstantaneousResponse eval(long ts) {
        this.execution = QueryExecutionFactory.create(getQuery(), getDataset());
        this.last_response = new ConstructResponse("http://streamreasoning.org/jasper/", query, execution.execConstruct(), ts);
        return s2r.eval(last_response);
    }
}
