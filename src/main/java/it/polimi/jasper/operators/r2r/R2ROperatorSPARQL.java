package it.polimi.jasper.operators.r2r;

import it.polimi.jasper.querying.results.SolutionMappingImpl;
import it.polimi.yasper.core.operators.r2r.RelationToRelationOperator;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.result.SolutionMapping;
import it.polimi.yasper.core.sds.SDS;
import lombok.extern.log4j.Log4j;
import org.apache.jena.ext.com.google.common.collect.Streams;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Context;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Log4j
public class R2ROperatorSPARQL implements RelationToRelationOperator<Binding>, QueryExecution {

    private final ContinuousQuery query;
    private final SDS sds;
    private final String baseURI;
    private final Dataset ds;
    private final Query q;
    public final List<String> resultVars;
    private QueryExecution execution;

    public R2ROperatorSPARQL(ContinuousQuery query, SDS sds, String baseURI) {
        this.query = query;
        this.sds = sds;
        this.baseURI = baseURI;
        this.ds = (Dataset) sds;
        this.q = (Query) this.query;
        resultVars = q.getResultVars();
    }

    @Override
    public Stream<SolutionMapping<Binding>> eval(long ts) {
        //TODO fix up to stream
        String id = baseURI + "result/" + ts;
        this.execution = QueryExecutionFactory.create(q, ds);
        return Streams.stream(this.execution.execSelect()).map(querySolution -> ((org.apache.jena.sparql.core.ResultBinding) querySolution).getBinding()).map(b -> new SolutionMappingImpl(id, b, this.resultVars, ts));
    }

    private List<Binding> getSolutionSet(ResultSet results) {

        List<Binding> solutions = new ArrayList<>();
        while (results.hasNext()) {
            solutions.add(results.nextBinding());
        }
        return solutions;
    }

    @Override
    public void setInitialBinding(QuerySolution binding) {

    }

    @Override
    public Dataset getDataset() {
        return (Dataset) sds;
    }

    @Override
    public Context getContext() {
        return null;
    }

    @Override
    public Query getQuery() {
        return q;
    }

    @Override
    public ResultSet execSelect() {
        return execution.execSelect();
    }

    @Override
    public Model execConstruct() {
        return execution.execConstruct();
    }

    @Override
    public Model execConstruct(Model model) {
        return execution.execConstruct(model);
    }

    @Override
    public Iterator<Triple> execConstructTriples() {
        return execution.execConstructTriples();
    }

    @Override
    public Iterator<Quad> execConstructQuads() {
        return execution.execConstructQuads();
    }

    @Override
    public Dataset execConstructDataset() {
        return execution.execConstructDataset();
    }

    @Override
    public Dataset execConstructDataset(Dataset dataset) {
        return execution.execConstructDataset(dataset);
    }

    @Override
    public Model execDescribe() {
        return execution.execDescribe();
    }

    @Override
    public Model execDescribe(Model model) {
        return execution.execDescribe(model);
    }

    @Override
    public Iterator<Triple> execDescribeTriples() {
        return execution.execDescribeTriples();
    }

    @Override
    public boolean execAsk() {
        return execution.execAsk();
    }

    @Override
    public void abort() {
        execution.abort();
    }

    @Override
    public void close() {
        execution.close();

    }

    @Override
    public boolean isClosed() {
        return execution.isClosed();
    }

    @Override
    public void setTimeout(long timeout, TimeUnit timeoutUnits) {
        execution.setTimeout(timeout, timeoutUnits);
    }

    @Override
    public void setTimeout(long timeout) {
        execution.setTimeout(timeout);
    }

    @Override
    public void setTimeout(long timeout1, TimeUnit timeUnit1, long timeout2, TimeUnit timeUnit2) {
        execution.setTimeout(timeout1, timeUnit1, timeout2, timeUnit2);
    }

    @Override
    public void setTimeout(long timeout1, long timeout2) {
        execution.setTimeout(timeout1, timeout2);
    }

    @Override
    public long getTimeout1() {
        return execution.getTimeout1();
    }

    @Override
    public long getTimeout2() {
        return execution.getTimeout2();
    }
}
