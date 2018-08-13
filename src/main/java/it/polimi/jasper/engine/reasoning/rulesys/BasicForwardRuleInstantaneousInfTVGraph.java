package it.polimi.jasper.engine.reasoning.rulesys;

import org.apache.jena.graph.Graph;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.BasicForwardRuleInfGraph;
import org.apache.jena.reasoner.rulesys.Rule;

import java.util.List;

/**
 * Created by riccardo on 05/07/2017.
 */
public class BasicForwardRuleInstantaneousInfTVGraph extends BasicForwardRuleInfGraph implements InfGraph {

    public BasicForwardRuleInstantaneousInfTVGraph(Reasoner reasoner, List<Rule> rules, Graph schema) {
        super(reasoner, rules, schema);
    }

}
