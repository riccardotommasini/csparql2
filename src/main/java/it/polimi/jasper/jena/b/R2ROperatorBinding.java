package it.polimi.jasper.jena.b;

import it.polimi.jasper.jena.SolutionMappingImpl;
import it.polimi.jasper.jena.syntax.RSPQLJenaQuery;
import it.polimi.yasper.core.operators.r2r.RelationToRelationOperator;
import it.polimi.yasper.core.querying.result.SolutionMapping;
import it.polimi.yasper.core.sds.timevarying.TimeVarying;
import lombok.extern.log4j.Log4j;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingUtils;

import java.util.*;
import java.util.stream.Stream;

@Log4j
public class R2ROperatorBinding implements RelationToRelationOperator<Binding> {

    private final RSPQLJenaQuery query;
    private final JenaSDSBB sds;
    private final String baseURI;
    public final List<String> resultVars;
    private QueryExecution execution;

    public R2ROperatorBinding(RSPQLJenaQuery query, JenaSDSBB sds, String baseURI) {
        this.query = query;
        this.sds = sds;
        this.baseURI = baseURI;
        this.resultVars = query.getResultVars();
    }

    @Override
    public Stream<SolutionMapping<Binding>> eval(long ts) {
        //TODO fix up to stream
        String id = baseURI + "result/" + ts;

        Set<Binding> answers = new HashSet<>();
        //suppose materialized at ts

        sds.tvgs().stream().filter(TimeVarying::named).forEach(tvb ->
                tvb.get().forEach(binding ->
                        {
                            sds.tvgs().stream().filter(TimeVarying::named)
                                    .filter(stringBindingSetEntry -> !stringBindingSetEntry.equals(tvb.iri()))//take the other windows
                                    .map(TimeVarying::get)
                                    .flatMap(Collection::stream)//unroll the binding sets
                                    .map(b -> BindingUtils.merge(binding, b))//merge line-by-line with current
                                    .forEach(answers::add);//add to solutions
                        }
                ));

        return answers.stream().map(binding -> new SolutionMappingImpl(id, binding, this.resultVars, ts));
    }

    private List<Binding> getSolutionSet(ResultSet results) {

        List<Binding> solutions = new ArrayList<>();
        while (results.hasNext()) {
            solutions.add(results.nextBinding());
        }
        return solutions;
    }


}
