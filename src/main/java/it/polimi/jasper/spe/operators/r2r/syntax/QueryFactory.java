package it.polimi.jasper.spe.operators.r2r.syntax;

import it.polimi.yasper.core.spe.operators.r2r.syntax.CaseChangingCharStream;
import it.polimi.yasper.core.spe.operators.r2r.syntax.RSPQLLexer;
import it.polimi.yasper.core.spe.operators.r2r.syntax.RSPQLParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.jena.riot.system.IRIResolver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class QueryFactory {
    public static RSPQLJenaQuery parse(IRIResolver resolver, String queryString) throws IOException {
        InputStream inputStream = new ByteArrayInputStream(queryString.getBytes());
        return parse(resolver, inputStream);
    }

    public static RSPQLJenaQuery parse(IRIResolver resolver, InputStream inputStream) throws IOException {
        // Ignore case for keywords
        CaseChangingCharStream charStream = new CaseChangingCharStream(CharStreams.fromStream(inputStream), true);
        RSPQLLexer lexer = new RSPQLLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RSPQLParser parser = new RSPQLParser(tokens);
        parser.setErrorHandler(new DefaultErrorStrategy());
        ParseTree tree = parser.queryUnit();
        RSPQLJenaQuery query = new RSPQLJenaQuery(resolver);
        RSPQLJenaVisitor visitor = new RSPQLJenaVisitor(query);
        visitor.visit(tree);
        return query;
    }
}