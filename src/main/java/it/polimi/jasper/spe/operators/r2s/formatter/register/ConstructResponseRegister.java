package it.polimi.jasper.spe.operators.r2s.formatter.register;

import java.io.ByteArrayInputStream;
import java.time.Instant;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import it.polimi.jasper.spe.operators.r2s.formatter.ConstructResponseDefaultFormatter;
import it.polimi.jasper.streams.RegisteredEPLStream;

public class ConstructResponseRegister extends ConstructResponseDefaultFormatter {
	
	private RegisteredEPLStream stream;

	public ConstructResponseRegister(String format, boolean distinct, RegisteredEPLStream stream) throws Throwable {
		super(format, distinct);
		if (format != "RDF/XML")
			throw new Throwable("Format must be RDF/XML");    
	    this.stream = stream;
	}

	@Override
	protected void out(String s) {
		
		Model model = ModelFactory.createDefaultModel();
        model.read(new ByteArrayInputStream(s.getBytes()), null);
        
        if (s != null)
            stream.put(model.getGraph(), Instant.now().toEpochMilli());
        
	}

}
