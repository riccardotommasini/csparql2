package it.polimi.jasper.rspql.querying.response;

import it.polimi.yasper.core.spe.operators.r2r.ContinuousQuery;
import it.polimi.yasper.core.spe.operators.r2s.result.InstantaneousResult;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;


@Getter
@Log4j
public final class ConstructResponse extends InstantaneousResult {
    private Model results;

    public ConstructResponse(String id, ContinuousQuery query, Model results, long cep_timestamp) {
        super(id, System.currentTimeMillis(), cep_timestamp, query);
        this.results = results;
    }

    private String getData() {

        String eol = System.getProperty("line.separator");
        String trig = getId() + " {";
        StmtIterator listStatements = results.listStatements();
        while (listStatements.hasNext()) {
            Statement s = listStatements.next();
            trig += eol + "<" + s.getSubject().toString() + ">" + " " + "<" + s.getPredicate().toString() + ">" + " " + "<"
                    + s.getObject().toString() + "> .";
        }
        trig += eol + "}" + eol;
        return trig;
    }

    @Override
    public ConstructResponse difference(InstantaneousResult r) {
        return new ConstructResponse(getId(), getQuery(), results.difference(((ConstructResponse) r).getResults()), getCep_timestamp());
    }

    @Override
    public InstantaneousResult intersection(InstantaneousResult new_response) {
        Model i;
        if (new_response == null) {
            i = ModelFactory.createDefaultModel();
        } else {
            Model r = ((ConstructResponse) new_response).getResults();
            i = this.results.intersection(r);
            i = r.difference(i);
        }
        return new ConstructResponse(getId(), getQuery(), i, getCep_timestamp());
    }
}
