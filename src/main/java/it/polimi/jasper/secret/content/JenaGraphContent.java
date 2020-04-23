package it.polimi.jasper.secret.content;

import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;

@Log4j
public class JenaGraphContent extends ContentEventBean<Graph, Graph, Graph> {

    public JenaGraphContent(Graph graph) {
        super(graph);
    }

    @Override
    public Graph coalesce() {
        content.clear();
        elements.forEach(ig -> GraphUtil.addInto(this.content, ig));
        //        elements.stream().flatMap(ig->GraphUtil.findAll(ig).toList().stream()).forEach(this.graph::add);

        return this.content;
    }

    public void replace(Graph coalesce) {
        this.content.clear();
        GraphUtil.addInto(content, coalesce);
    }
}
