package it.polimi.seraph;

import it.polimi.yasper.core.querying.result.SolutionMapping;
import it.polimi.yasper.core.querying.result.SolutionMappingBase;
import lombok.Getter;
import lombok.extern.java.Log;

import java.util.List;
import java.util.Map;

@Log
@Getter
public final class SolutionMappingImplNeo4j extends SolutionMappingBase<Map<String, Object>> {

    private final List<String> result_vars;

    public SolutionMappingImplNeo4j(String id, Map<String, Object> results, List<String> resultVars, long cep_timestamp) {
        super(id, System.currentTimeMillis(), cep_timestamp, results);
        this.result_vars = resultVars;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SolutionMappingImplNeo4j response = (SolutionMappingImplNeo4j) o;
        Map<String, Object> binding = response.get();
        return this.get().equals(binding);
    }

    @Override
    public int hashCode() {
        return result_vars != null ? result_vars.hashCode() : 0;
    }

    @Override
    public SolutionMapping<Map<String, Object>> difference(SolutionMapping<Map<String, Object>> r) {
        //todo
        return null;
    }

    @Override
    public SolutionMapping<Map<String, Object>> intersection(SolutionMapping<Map<String, Object>> new_response) {
        //todo
        return null;
    }
}
