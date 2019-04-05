package it.polimi.jasper.spe.operators.r2r.execution;

import it.polimi.jasper.rspql.reasoning.Entailment;
import it.polimi.jasper.rspql.sds.JenaSDS;
import it.polimi.jasper.spe.operators.r2r.syntax.RSPQLJenaQuery;
import it.polimi.yasper.core.spe.operators.r2r.execution.ContinuousQueryExecutionObserver;
import it.polimi.yasper.core.spe.operators.r2s.*;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by riccardo on 04/07/2017.
 */
public final class ContinuousQueryExecutionFactory extends QueryExecutionFactory {


    static public ContinuousQueryExecutionObserver create(RSPQLJenaQuery query, JenaSDS sds) {
        ContinuousQueryExecutionObserver cqe;
        StreamOperator r2S = query.getR2S() != null ? query.getR2S() : StreamOperator.RSTREAM;
        RelationToStreamOperator s2r = getToStreamOperator(r2S);

        if (query.isSelectType()) {
            cqe = new ContinuousSelect(query, sds, s2r);
        } else if (query.isConstructType()) {
            cqe = new ContinuouConstruct(query, sds, s2r);
        } else {
            throw new RuntimeException("Unsupported ContinuousQuery Type ");
        }

        return cqe;
    }


    private static RelationToStreamOperator getToStreamOperator(StreamOperator r2S) {
        switch (r2S) {
            case DSTREAM:
                return new Dstream(1);
            case ISTREAM:
                return new Istream(1);
            case RSTREAM:
                return new Rstream();
            default:
                return new Rstream();
        }
    }


    public static Reasoner getReasoner(Entailment et, List<Rule> rules, Model tbox) {
        switch (et) {
            case OWL:
                return ReasonerRegistry.getOWLReasoner().bindSchema(tbox);
            case RDFS:
                return ReasonerRegistry.getRDFSReasoner().bindSchema(tbox);
            case OWL2RL:
                return ReasonerRegistry.getRDFSReasoner().bindSchema(tbox);
            case CUSTOM:
                return getTvgReasoner(tbox, rules);
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
