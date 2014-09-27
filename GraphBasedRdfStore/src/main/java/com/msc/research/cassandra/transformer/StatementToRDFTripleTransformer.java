package com.msc.research.cassandra.transformer;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Statement;
import com.msc.research.cassandra.model.RDFTriple;

public class StatementToRDFTripleTransformer implements
		DataTransformer<List<Statement>, List<RDFTriple>> {

	@Override
	public List<RDFTriple> transform(List<Statement> source) {
		List<RDFTriple> triples = new ArrayList<RDFTriple>(source.size());
		for (Statement statement : source) {
			RDFTriple triple = new RDFTriple(statement.getSubject()
					.getLocalName(), statement.getPredicate().getLocalName(),
					statement.getString());

			triples.add(triple);
		}
		return triples;
	}

}
