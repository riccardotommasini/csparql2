package it.polimi.jasper.jena.syntax;

import it.polimi.yasper.core.querying.syntax.RSPQLParser;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.modify.request.QuadAcc;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementNamedGraph;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.syntax.Template;

import java.time.Duration;
import java.util.ArrayList;

/**
 * This parser class is based on the RSP-QL syntax described using ANTRL4. The parse tree visitor maps the static
 * syntax parts of the syntax to an extended Jena query. The visitor is based on the SPARQLJenaVisitor.
 */

public class RSPQLJenaVisitor extends SPARQL11JenaVisitor {
    private RSPQLJenaQuery rootQuery;
    private RSPQLJenaQuery query;

    public RSPQLJenaVisitor(RSPQLJenaQuery query) {
        super(query);
        rootQuery = query;
        this.query = query;
    }

    public Object visitOutputStreamType(RSPQLParser.OutputStreamTypeContext ctx) {
        switch (ctx.getText()) {
            case "ISTREAM":
                query.setIstream();
                break;
            case "DSTREAM":
                query.setDstream();
                break;
            case "RSTREAM":
            default:
                query.setRstream();
                break;
        }
        return null;
    }

    public Object visitOutputStream(RSPQLParser.OutputStreamContext ctx) {
        String uri = ctx.sourceSelector().accept(this).toString();
        query.setOutputStream(uri);
        return null;
    }

    public Object visitNamedWindowClause(RSPQLParser.NamedWindowClauseContext ctx) {
        Node windowUri = (Node) ctx.windowUri().accept(this);
        Node streamUri = (Node) ctx.streamUri().accept(this);
        if (ctx.window().logicalWindow() != null) {
            RSPQLParser.LogicalWindowContext c = ctx.window().logicalWindow();
            Duration range = Duration.parse(c.logicalRange().duration().getText());
            Duration step = null;
            if (c.logicalStep() != null) {
                step = Duration.parse(c.logicalStep().duration().getText());
            }
            query.addNamedWindow(windowUri, streamUri, range, step);
        } else if (ctx.window().physicalWindow() != null) {
            RSPQLParser.PhysicalWindowContext c = ctx.window().physicalWindow();
            int range = Integer.parseInt(c.physicalRange().integer().getText());
            int step = -1;
            if (c.physicalStep() != null) {
                step = Integer.parseInt(c.physicalStep().integer().getText());
            }
            query.addNamedWindow(windowUri, streamUri, range, step);
        }
        query.addNamedGraphURI(windowUri.toString()); // add window as named graph to Jena query
        return null;
    }

    public ElementNamedGraph visitWindowGraphPattern(RSPQLParser.WindowGraphPatternContext ctx) {
        Node n = (Node) ctx.varOrIri().accept(this);
        ElementGroup elg = (ElementGroup) ctx.groupGraphPattern().accept(this);
        ElementNamedWindow elementNamedWindow = new ElementNamedWindow(n, elg);
        query.addElementNamedWindow(elementNamedWindow);
        return new ElementNamedGraph(n, elg);
    }

    @Override
    public Object visitConstructTemplate(RSPQLParser.ConstructTemplateContext ctx) {
        ArrayList<Quad> quads = new ArrayList<>();
        if (ctx.quads().quadsNotTriples() != null) {
            ctx.quads().quadsNotTriples().forEach(graph -> {
                Node n = (Node) graph.varOrIri().accept(this);
                ElementTriplesBlock etb = (ElementTriplesBlock) graph.triplesTemplate().accept(this);
                etb.patternElts().forEachRemaining(t -> {
                    Quad q = new Quad(n, t);
                    quads.add(q);
                });
            });
        }

        // Triples must be added as quads
        if (ctx.quads().triplesTemplate() != null) {
            ctx.quads().triplesTemplate().forEach(triplesTemplate -> {
                ElementTriplesBlock etb = (ElementTriplesBlock) triplesTemplate.accept(this);
                etb.getPattern().forEach(triple -> {
                    quads.add(new Quad(Quad.defaultGraphNodeGenerated, triple));
                });
            });
        }

        Template t = new Template(new QuadAcc(quads));
        query.setConstructTemplate(t);
        return null;
    }
}

