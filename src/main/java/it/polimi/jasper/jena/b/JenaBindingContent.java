package it.polimi.jasper.jena.b;

import it.polimi.jasper.secret.content.ContentEventBean;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.ModelFactory;

@Log4j
public class JenaBindingContent extends ContentEventBean<Triple, Graph, BindingSet> {

    private Query query;
    private BindingSet set;

    public JenaBindingContent() {
        super(new GraphMem());
        set = new BindingSet();
    }

//    protected void handleSingleIStream(StreamItem<Triple> st) {
//        // log.debug("Handling single IStreamTest [" + st + "]");
//        Graph graphMem = GraphFactory.createGraphMem();
//        graphMem.add(st.getTypedContent());
//        QueryExecution cqe = QueryExecutionFactory.create(query, ModelFactory.createModelForGraph(graphMem));
//
//        elements.add(cqe.execSelect().nextBinding());
//    }

    public BindingSet coalesce() {
        elements.forEach(content::add);
        ResultSet resultSet = QueryExecutionFactory.create(query, ModelFactory.createModelForGraph(content), set.asQuerySolutionSet()).execSelect();
        while (resultSet.hasNext())
            this.set.add(resultSet.nextBinding());
        return this.set;
    }

    @Override
    public void replace(BindingSet coalesce) {
        this.set = coalesce;
    }
}
