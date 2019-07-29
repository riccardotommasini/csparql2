package it.polimi.jasper.operators.r2s;

import it.polimi.yasper.core.operators.r2s.RelationToStreamOperator;
import org.apache.jena.sparql.engine.binding.Binding;
import it.polimi.yasper.core.querying.result.SolutionMapping;

public class JRStream implements RelationToStreamOperator<Binding> {

    @Override
    public Binding eval(SolutionMapping<Binding> last_response, long ts) {
        return last_response.get();
    }
}