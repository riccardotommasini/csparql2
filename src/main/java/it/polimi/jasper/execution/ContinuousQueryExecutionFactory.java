package it.polimi.jasper.execution;

import it.polimi.jasper.querying.Entailment;
import it.polimi.jasper.sds.JenaSDS;
import it.polimi.jasper.operators.r2r.R2ROperatorSPARQL;
import it.polimi.jasper.querying.syntax.RSPQLJenaQuery;
import it.polimi.jasper.operators.r2s.JDStream;
import it.polimi.jasper.operators.r2s.JIStream;
import it.polimi.jasper.operators.r2s.JRStream;
import it.polimi.yasper.core.enums.StreamOperator;
import it.polimi.yasper.core.operators.r2r.RelationToRelationOperator;
import it.polimi.yasper.core.operators.r2s.*;
import it.polimi.yasper.core.operators.s2r.StreamToRelationOperator;
import it.polimi.yasper.core.stream.data.WebDataStream;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by riccardo on 04/07/2017.
 */
public final class ContinuousQueryExecutionFactory extends QueryExecutionFactory {


    static public JenaContinuousQueryExecution create(IRIResolver resolver, RSPQLJenaQuery query, JenaSDS sds, WebDataStream out, StreamToRelationOperator... s2rs) {
        StreamOperator r2S = query.getR2S() != null ? query.getR2S() : StreamOperator.RSTREAM;
        RelationToRelationOperator<Binding> r2r = new R2ROperatorSPARQL(query, sds, resolver.getBaseIRIasString());
        RelationToStreamOperator s2r = getToStreamOperator(r2S);
        return new JenaContinuousQueryExecution(resolver, out, query, sds, r2r, s2r, s2rs);
    }

    private static RelationToStreamOperator getToStreamOperator(StreamOperator r2S) {
        switch (r2S) {
            case DSTREAM:
                return new JDStream(1);
            case ISTREAM:
                return new JIStream(1);
            case RSTREAM:
                return new JRStream();
            default:
                return new JRStream();
        }
    }


    public static Reasoner getReasoner(Entailment et, List<Rule> rules, String tboxLocation) {
        switch (et) {
            case OWL:
                return ReasonerRegistry.getOWLReasoner().bindSchema(ModelFactory.createDefaultModel().read(tboxLocation));
            case RDFS:
                return ReasonerRegistry.getRDFSReasoner().bindSchema(ModelFactory.createDefaultModel().read(tboxLocation));
            case OWL2RL:
                return ReasonerRegistry.getRDFSReasoner().bindSchema(ModelFactory.createDefaultModel().read(tboxLocation));
            case CUSTOM:
                return getTvgReasoner(ModelFactory.createDefaultModel().read(tboxLocation), rules);
            case NONE:
            default:
                return null;

        }
    }

    private static GenericRuleReasoner getTvgReasoner(Model tbox, List<Rule> rules) {
        GenericRuleReasoner reasoner = new GenericRuleReasoner(rules);
        reasoner.setMode(GenericRuleReasoner.HYBRID);
        return (GenericRuleReasoner) reasoner.bindSchema(tbox);
    }

    public static GenericRuleReasoner emptyReasoner() {
        return getTvgReasoner(ModelFactory.createDefaultModel(), new ArrayList<>());
    }
}
