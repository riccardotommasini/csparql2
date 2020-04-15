package it.polimi.seraph.streans;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class PGraph {

    private List<Node> nodes;
    private List<Relationship> edges;

    public PGraph(List<Node> nodes, List<Relationship> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }

    public PGraph() {
        this.nodes=new ArrayList<>();
        this.edges=new ArrayList<>();
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public List<Relationship> getEdges() {
        return edges;
    }

    public void setEdges(List<Relationship> edges) {
        this.edges = edges;
    }

    public void clear() {
        nodes.clear();
        edges.clear();
    }

    public void addAll(PGraph ig) {
        nodes.addAll(ig.getNodes());
        edges.addAll(ig.getEdges());
    }
}
