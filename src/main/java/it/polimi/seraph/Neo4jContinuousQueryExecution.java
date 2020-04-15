package it.polimi.seraph;

import it.polimi.seraph.streans.PGraph;
import it.polimi.yasper.core.format.QueryResultFormatter;
import it.polimi.yasper.core.operators.r2r.RelationToRelationOperator;
import it.polimi.yasper.core.operators.r2s.RelationToStreamOperator;
import it.polimi.yasper.core.operators.s2r.StreamToRelationOperator;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.querying.result.SolutionMapping;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.stream.data.WebDataStream;
import lombok.extern.log4j.Log4j;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.syntax.Template;

import java.util.*;
import java.util.stream.Stream;

/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j
public class Neo4jContinuousQueryExecution extends Observable implements Observer, ContinuousQueryExecution<PGraph, PGraph, Map<String, Object>> {

    private final RelationToStreamOperator<Binding> r2s;
    private final RelationToRelationOperator<Binding> r2r;
    private final SDS sds;
    private final ContinuousQuery query;
    private final Query q;
    private final Template template;
    private final WebDataStream out;
    private List<StreamToRelationOperator<Graph, Graph>> s2rs;
    protected QueryExecution execution;
    protected IRIResolver resolver;

    public Neo4jContinuousQueryExecution(IRIResolver resolver, WebDataStream out, ContinuousQuery query, SDS sds, RelationToRelationOperator<Binding> r2r, RelationToStreamOperator<Binding> r2s, StreamToRelationOperator<Graph, Graph>... s2rs) {
        this.resolver = resolver;
        this.query = query;
        this.q = (Query) query;
        this.template = q.getConstructTemplate();
        this.sds = sds;
        this.s2rs = s2rs == null ? new ArrayList<>() : Arrays.asList(s2rs);
        this.r2r = r2r;
        this.r2s = r2s;
        this.out = out;
    }

    @Override
    public void update(Observable o, Object arg) {
        Long now = (Long) arg;
        sds.materialize(now);
        Stream<SolutionMapping<Binding>> eval1 = r2r.eval(now);
        eval1.forEach(ib -> {
            Binding eval = r2s.eval(ib, now);
            setChanged();
            Table arg1 = apply2(eval, now);
            if (outstream() != null) {
                outstream().put(arg1, now);
            }
            notifyObservers(arg1);
        });
    }

    @Override
    public <T> WebDataStream<T> outstream() {
        return out;
    }

    private Table apply2(Binding eval, Long now) {
        Table table = TableFactory.create();

        Node etime = NodeFactory.createLiteral(now + "", XSDDatatype.XSDdateTimeStamp);
        Binding timestamp = BindingFactory.binding(Var.alloc("eventTime"), etime);
        Binding merge = Algebra.merge(eval, timestamp);
        Node ptime = NodeFactory.createLiteral(System.currentTimeMillis() + "", XSDDatatype.XSDdateTimeStamp);
        timestamp = BindingFactory.binding(Var.alloc("processingTime"), ptime);
        merge = Algebra.merge(merge, timestamp);
        table.addBinding(merge);
        return table;
    }


    @Override
    public ContinuousQuery getContinuousQuery() {
        return query;
    }

    @Override
    public SDS<PGraph> getSDS() {
        return null;
    }

    @Override
    public StreamToRelationOperator<PGraph, PGraph>[] getS2R() {
        return new StreamToRelationOperator[0];
    }


    public void addS2R(StreamToRelationOperator<Graph, Graph> op) {
        s2rs.add(op);
    }


    @Override
    public RelationToRelationOperator getR2R() {
        return r2r;
    }

    @Override
    public RelationToStreamOperator getR2S() {
        return r2s;
    }


    @Override
    public void add(QueryResultFormatter o) {
        addObserver(o);
    }

    @Override
    public void remove(QueryResultFormatter o) {
        deleteObserver(o);
    }

}
