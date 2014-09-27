package com.msc.research.cassandra.transformer;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.msc.research.cassandra.model.RDFTriple;

/**
 * Transforms a {@link ResultSet} returned by Cassandra into a list of
 * {@link RDFTriple} instances.
 * 
 * @author ravindra
 *
 */
public class ResultSetToRDFTripleTransformer implements
		DataTransformer<ResultSet, List<RDFTriple>> {

	@Override
	public List<RDFTriple> transform(ResultSet source) {
		List<RDFTriple> rdfTriples = new ArrayList<RDFTriple>();
		for (Row row : source) {
			rdfTriples.add(new RDFTriple(row.getString("subject"), row
					.getString("predicate"), row.getString("object")));
		}
		return rdfTriples;
	}

}
