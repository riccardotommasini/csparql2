package it.polimi.jasper.rspql.querying.execution;

import it.polimi.jasper.rspql.querying.response.SelectResponse;
import it.polimi.yasper.core.rspql.operators.r2s.RelationToStreamOperator;
import it.polimi.yasper.core.rspql.querying.ContinuousQuery;
import it.polimi.yasper.core.rspql.response.InstantaneousResponse;
import it.polimi.yasper.core.rspql.sds.SDS;
import org.apache.jena.query.QueryExecutionFactory;

/**
 * Created by riccardo on 03/07/2017.
 */
public class ContinuousSelect extends JenaContinuousQueryExecution {

    public ContinuousSelect(ContinuousQuery query, SDS sds, RelationToStreamOperator s2r) {
        super(query, sds, s2r);
    }


    @Override
    public InstantaneousResponse eval(long ts) {
        this.execution = QueryExecutionFactory.create(getQuery(), getDataset());
        this.last_response = new SelectResponse("http://streamreasoning.org/jasper/", query, execution.execSelect(), q.getResultVars(), ts);
        return s2r.eval(last_response);
    }
}
