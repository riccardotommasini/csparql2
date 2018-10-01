package it.polimi.jasper.spe.operators.r2s.formatter;

import com.github.jsonldjava.core.JsonLdOptions;
import it.polimi.jasper.spe.operators.r2s.results.SelectResponse;
import it.polimi.yasper.core.spe.operators.r2s.result.QueryResultFormatter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.JsonLDWriteContext;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapStd;
import org.apache.jena.riot.writer.JsonLDWriter;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.resultset.RDFOutput;

import java.io.ByteArrayOutputStream;
import java.util.Observable;

/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j
public abstract class SelectResponseDefaultFormatter extends QueryResultFormatter {

    private JsonLDWriter jsonLDWriter = new JsonLDWriter(RDFFormat.JSONLD_FLATTEN_PRETTY);
    private long last_result = -1L;
    private PrefixMap pm = new PrefixMapStd();
    private JsonLDWriteContext context = new JsonLDWriteContext();
    private JsonLdOptions options = new JsonLdOptions();


    public SelectResponseDefaultFormatter(String format, boolean distinct) {
        super(format, distinct);
        options.setPruneBlankNodeIdentifiers(true);
        context.setOptions(options);
    }

    @Override
    public void update(Observable o, Object arg) {
        SelectResponse sr = (SelectResponse) arg;
        this.format(sr);
    }

    public void format(SelectResponse sr) {
        long cep_timestamp = sr.getCep_timestamp();
        if (cep_timestamp != last_result && distinct) {
            last_result = cep_timestamp;
            log.debug("[" + System.currentTimeMillis() + "] Result at [" + last_result + "]");

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ResultSetRewindable results = sr.getResults();

            switch (format) {
                case "CSV":
                    ResultSetFormatter.outputAsCSV(outputStream, results);
                    break;
                case "JSON-LD":
                    Model model = RDFOutput.encodeAsModel(results);
                    model.getNsPrefixMap().forEach(pm::add);
                    jsonLDWriter.write(outputStream, DatasetGraphFactory.create(model.getGraph()), pm, "", context);
                    break;
                case "JSON":
                    ResultSetFormatter.outputAsJSON(outputStream, results);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid Format: " + format);
            }

            out(new String(outputStream.toByteArray()));
        }
    }

    protected abstract void out(String s);

}
