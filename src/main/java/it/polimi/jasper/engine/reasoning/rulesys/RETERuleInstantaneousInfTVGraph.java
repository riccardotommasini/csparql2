package it.polimi.jasper.engine.reasoning.rulesys;

import org.apache.jena.graph.Graph;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.RETERuleInfGraph;
import org.apache.jena.reasoner.rulesys.Rule;

import java.util.List;

/**
 * Created by riccardo on 05/07/2017.
 */
public class RETERuleInstantaneousInfTVGraph extends RETERuleInfGraph implements InfGraph {

    public RETERuleInstantaneousInfTVGraph(Reasoner reasoner, List<Rule> rules, Graph schema) {
        super(reasoner, rules, schema);
    }


}
