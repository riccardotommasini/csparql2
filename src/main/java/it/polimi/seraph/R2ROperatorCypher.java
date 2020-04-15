package it.polimi.seraph;

import it.polimi.yasper.core.operators.r2r.RelationToRelationOperator;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.result.SolutionMapping;
import it.polimi.yasper.core.sds.SDS;
import lombok.extern.log4j.Log4j;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.engine.binding.Binding;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Log4j
public class R2ROperatorCypher implements RelationToRelationOperator<Map<String, Object>> {

    private final ContinuousQuery query;
    private final SDS sds;
    private final String baseURI;
    public final List<String> resultVars;
    private QueryExecution execution;

//    private static final File databaseDirectory = new File(R2ROperatorCypher.class.getResource("db").getPath());

    //    DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(databaseDirectory).build();
//    GraphDatabaseService db = managementService.database(DEFAULT_DATABASE_NAME);
    TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder();
    GraphDatabaseService db = builder.impermanent().build().database(DEFAULT_DATABASE_NAME);


    private Transaction tx;

    public R2ROperatorCypher(ContinuousQuery query, SDS sds, String baseURI) {
        this.query = query;
        this.sds = sds;
        this.baseURI = baseURI;
        resultVars = query.getResultVars();
    }

    @Override
    public Stream<SolutionMapping<Map<String, Object>>> eval(long ts) {
        //TODO fix up to stream
        String id = baseURI + "result;" + ts;
        this.tx = db.beginTx();
        Result result = tx.execute(query.getSPARQL());
//        |--name-|--age--|-email--|
//        |--Fred--|--22--|--null--|
//        |--Riccardo--|--29--|--null--|

        List<Map<String, Object>> res = new ArrayList<>();
        while (result.hasNext()) {
            Map<String, Object> next = result.next();
            res.add(next);
//        |name-->Fred
//        |age-->22
        }

        tx.commit();

        return res.stream().map(b -> new SolutionMappingImplNeo4j(id, b, this.resultVars, ts));
    }

    private List<Binding> getSolutionSet(ResultSet results) {

        List<Binding> solutions = new ArrayList<>();
        while (results.hasNext()) {
            solutions.add(results.nextBinding());
        }
        return solutions;
    }


}
