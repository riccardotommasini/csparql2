package it.polimi.seraph.r2s;

import it.polimi.yasper.core.operators.r2s.RelationToStreamOperator;
import it.polimi.yasper.core.querying.result.SolutionMapping;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.Map;

public class JRStreamNeo4J implements RelationToStreamOperator<Map<String,Object>> {

    @Override
    public Map<String,Object> eval(SolutionMapping<Map<String,Object>> last_response, long ts) {
        return last_response.get();
    }
}