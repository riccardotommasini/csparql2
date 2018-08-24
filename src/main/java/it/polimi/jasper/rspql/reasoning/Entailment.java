package it.polimi.jasper.rspql.reasoning;

import org.apache.jena.reasoner.rulesys.Rule;

import java.util.List;

public interface Entailment {
    List<Rule> getRules();
}
