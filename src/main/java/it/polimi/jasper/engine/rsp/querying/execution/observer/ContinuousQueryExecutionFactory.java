package it.polimi.jasper.engine.rsp.querying.execution.observer;

import it.polimi.jasper.engine.rsp.querying.execution.subscribers.ContinuouConstructSubscriber;
import it.polimi.jasper.engine.rsp.querying.execution.subscribers.ContinuousSelectSubscriber;
import it.polimi.yasper.core.enums.StreamOperator;
import it.polimi.yasper.core.quering.execution.ContinuousQueryExecutionObserver;
import it.polimi.yasper.core.quering.execution.ContinuousQueryExecutionSubscriber;
import it.polimi.yasper.core.quering.operators.r2s.Dstream;
import it.polimi.yasper.core.quering.operators.r2s.Istream;
import it.polimi.yasper.core.quering.operators.r2s.RelationToStreamOperator;
import it.polimi.yasper.core.quering.operators.r2s.Rstream;
import it.polimi.yasper.core.quering.querying.ContinuousQuery;
import it.polimi.yasper.core.quering.rspql.sds.SDS;
import it.polimi.yasper.core.reasoning.Entailment;
import it.polimi.yasper.core.reasoning.TVGReasoner;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by riccardo on 04/07/2017.
 */
public final class ContinuousQueryExecutionFactory extends QueryExecutionFactory {


    static public ContinuousQueryExecutionObserver createObserver(ContinuousQuery query, SDS sds) {
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

    static public ContinuousQueryExecutionSubscriber createSubscriber(ContinuousQuery query, SDS sds, TVGReasoner r) {
        ContinuousQueryExecutionSubscriber cqe;
        StreamOperator r2S = query.getR2S() != null ? query.getR2S() : StreamOperator.RSTREAM;
        RelationToStreamOperator s2r = getToStreamOperator(r2S);

        if (query.isSelectType()) {
            cqe = new ContinuousSelectSubscriber(query, sds, s2r);
        } else if (query.isConstructType()) {
            cqe = new ContinuouConstructSubscriber(query, sds, s2r);
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


    public static GenericRuleReasoner getGenericRuleReasoner(Entailment ent, Model tbox) {
        GenericRuleReasoner reasoner = null;
        switch (ent.getType()) {
            case OWL2DL:
            case OWL2EL:
            case OWL2QL:
            case OWL2RL:
            case CUSTOM:
            case PELLET:
                break;
            default:
                reasoner = getTvgReasoner(tbox, (List<Rule>) ent.getRules());
        }

        return reasoner;
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