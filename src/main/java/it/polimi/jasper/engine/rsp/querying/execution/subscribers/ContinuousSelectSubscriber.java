package it.polimi.jasper.engine.rsp.querying.execution.subscribers;

import it.polimi.jasper.engine.rsp.querying.response.ConstructResponse;
import it.polimi.yasper.core.quering.execution.ContinuousQueryExecutionSubscriber;
import it.polimi.yasper.core.quering.operators.r2s.RelationToStreamOperator;
import it.polimi.yasper.core.quering.querying.ContinuousQuery;
import it.polimi.yasper.core.quering.response.InstantaneousResponse;
import it.polimi.yasper.core.quering.rspql.sds.SDS;
import org.apache.jena.query.*;

import java.util.Observable;

/**
 * Created by riccardo on 03/07/2017.
 */
public class ContinuousSelectSubscriber extends ContinuousQueryExecutionSubscriber {

    private final Query q;
    protected InstantaneousResponse last_response = null;
    protected QueryExecution execution;

    public ContinuousSelectSubscriber(ContinuousQuery query, SDS sds, RelationToStreamOperator s2r) {
        super(query, sds, s2r);
        this.q = QueryFactory.create(query.getSPARQL());
    }


    @Override
    public InstantaneousResponse eval(long ts) {
        this.execution = QueryExecutionFactory.create(q, (Dataset) sds);
        this.last_response = new ConstructResponse("http://streamreasoning.org/yasper/", query, execution.execConstruct(), ts);
        return s2r.eval(last_response);
    }

    @Override
    public void update(Observable o, Object arg) {
        eval((Long) arg);
    }
}
