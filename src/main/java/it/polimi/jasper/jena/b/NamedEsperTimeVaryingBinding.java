package it.polimi.jasper.jena.b;

import it.polimi.jasper.secret.content.ContentEventBean;
import it.polimi.yasper.core.enums.Maintenance;
import it.polimi.yasper.core.operators.s2r.execution.assigner.Assigner;
import it.polimi.yasper.core.sds.SDS;
import it.polimi.yasper.core.secret.report.Report;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;


@Log4j
@Getter
public class NamedEsperTimeVaryingBinding extends EsperTimeVaryingBindingImpl {

    private String uri;

    public NamedEsperTimeVaryingBinding(ContentEventBean<Triple, Graph, BindingSet> c, String uri, Maintenance maintenance, Report report, Assigner<Triple, BindingSet> wo, SDS sds) {
        super(c, maintenance, report, wo, sds);
        this.uri = uri;
    }

    @Override
    public String iri() {
        return uri;
    }

    @Override
    public boolean named() {
        return true;
    }

}