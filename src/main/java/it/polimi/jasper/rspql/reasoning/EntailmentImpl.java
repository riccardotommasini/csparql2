package it.polimi.jasper.rspql.reasoning;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.jena.reasoner.rulesys.Rule;

import java.util.List;

@AllArgsConstructor
@Getter
public class EntailmentImpl implements Entailment {

    private final String id;
    private final List<Rule> rules;
    private final EntailmentType type;

}
