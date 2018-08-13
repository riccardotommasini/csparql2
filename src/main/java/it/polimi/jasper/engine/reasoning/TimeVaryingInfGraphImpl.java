package it.polimi.jasper.engine.reasoning;

import it.polimi.yasper.core.quering.TimeVarying;
import org.apache.jena.graph.*;
import org.apache.jena.reasoner.Derivation;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ValidityReport;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.Iterator;

public class TimeVaryingInfGraphImpl implements TimeVarying<InfGraph>, InfGraph {

    private final InfGraph infgraph;

    public TimeVaryingInfGraphImpl(InfGraph bind) {
        this.infgraph = bind;
    }

    @Override
    public InfGraph eval(long ts) {
        return null;
    }

    @Override
    public InfGraph asT() {
        return infgraph;
    }

    @Override
    public Graph getRawGraph() {
        return infgraph.getRawGraph();
    }

    @Override
    public Reasoner getReasoner() {
        return infgraph.getReasoner();
    }

    @Override
    public void rebind(Graph data) {
        infgraph.rebind(data);
    }

    @Override
    public void rebind() {
        infgraph.rebind();
    }

    @Override
    public void prepare() {
        infgraph.prepare();
    }

    @Override
    public void reset() {
        infgraph.reset();
    }

    @Override
    public Node getGlobalProperty(Node property) {
        return infgraph.getGlobalProperty(property);
    }

    @Override
    public boolean testGlobalProperty(Node property) {
        return infgraph.testGlobalProperty(property);
    }

    @Override
    public ValidityReport validate() {
        return infgraph.validate();
    }

    @Override
    public ExtendedIterator<Triple> find(Node subject, Node property, Node object, Graph param) {
        return infgraph.find(subject, property, object, param);
    }

    @Override
    public void setDerivationLogging(boolean logOn) {
        infgraph.setDerivationLogging(logOn);
    }

    @Override
    public Iterator<Derivation> getDerivation(Triple triple) {
        return infgraph.getDerivation(triple);
    }

    @Override
    public Graph getDeductionsGraph() {
        return infgraph.getDeductionsGraph();
    }

    @Override
    public boolean dependsOn(Graph other) {
        return infgraph.dependsOn(other);
    }

    @Override
    public TransactionHandler getTransactionHandler() {
        return infgraph.getTransactionHandler();
    }

    @Override
    public Capabilities getCapabilities() {
        return infgraph.getCapabilities();
    }

    @Override
    public GraphEventManager getEventManager() {
        return infgraph.getEventManager();
    }

    @Override
    public GraphStatisticsHandler getStatisticsHandler() {
        return infgraph.getStatisticsHandler();
    }

    @Override
    public PrefixMapping getPrefixMapping() {
        return infgraph.getPrefixMapping();
    }

    @Override
    public void add(Triple t) throws AddDeniedException {
        infgraph.add(t);
    }

    @Override
    public void delete(Triple t) throws DeleteDeniedException {
        infgraph.delete(t);
    }

    @Override
    public ExtendedIterator<Triple> find(Triple m) {
        return infgraph.find(m);
    }

    @Override
    public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
        return infgraph.find(s, p, o);
    }

    @Override
    public boolean isIsomorphicWith(Graph g) {
        return infgraph.isIsomorphicWith(g);
    }

    @Override
    public boolean contains(Node s, Node p, Node o) {
        return infgraph.contains(s, p, o);
    }

    @Override
    public boolean contains(Triple t) {
        return infgraph.contains(t);
    }

    @Override
    public void clear() {
        infgraph.clear();
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        infgraph.remove(s, p, o);
    }

    @Override
    public void close() {
        infgraph.close();
    }

    @Override
    public boolean isEmpty() {
        return infgraph.isEmpty();
    }

    @Override
    public int size() {
        return infgraph.size();
    }

    @Override
    public boolean isClosed() {
        return infgraph.isClosed();
    }

    @Override
    public String toString() {
        return infgraph.toString();
    }
}
