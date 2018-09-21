package it.polimi.jasper.spe.operators.r2r.execution;

import it.polimi.jasper.spe.operators.r2s.results.SelectResponse;
import it.polimi.yasper.core.rspql.sds.SDS;
import it.polimi.yasper.core.spe.operators.r2r.ContinuousQuery;
import it.polimi.yasper.core.spe.operators.r2s.RelationToStreamOperator;
import it.polimi.yasper.core.spe.operators.r2s.result.InstantaneousResult;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.query.QueryExecutionFactory;

import java.util.Iterator;

/**
 * Created by riccardo on 03/07/2017.
 */
public class ContinuousSelect extends JenaContinuousQueryExecution {

    public ContinuousSelect(ContinuousQuery query, SDS sds, RelationToStreamOperator s2r) {
        super(query, sds, s2r);
    }


    @Override
    public InstantaneousResult eval(long ts) {
        this.execution = QueryExecutionFactory.create(getQuery(), getDataset());
        this.last_response = new SelectResponse("http://streamreasoning.org/jasper/", query, execution.execSelect(), q.getResultVars(), ts);
        return s2r.eval(last_response);
    }
}
