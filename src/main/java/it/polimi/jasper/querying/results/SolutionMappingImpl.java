package it.polimi.jasper.querying.results;

import it.polimi.yasper.core.querying.result.SolutionMapping;
import it.polimi.yasper.core.querying.result.SolutionMappingBase;
import lombok.Getter;
import lombok.extern.java.Log;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.List;

@Log
@Getter
public final class SolutionMappingImpl extends SolutionMappingBase<Binding> {

    private final List<String> result_vars;

    public SolutionMappingImpl(String id, Binding results, List<String> resultVars, long cep_timestamp) {
        super(id, System.currentTimeMillis(), cep_timestamp, results);
        this.result_vars = resultVars;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SolutionMappingImpl response = (SolutionMappingImpl) o;
        Binding binding = response.get();
        return this.get().equals(binding);
    }

    @Override
    public int hashCode() {
        return result_vars != null ? result_vars.hashCode() : 0;
    }

    @Override
    public SolutionMapping<Binding> difference(SolutionMapping<Binding> r) {
        //todo
        return null;
    }

    @Override
    public SolutionMapping<Binding> intersection(SolutionMapping<Binding> new_response) {
        //todo
        return null;
    }
}
