package com.msc.research.cassandra.graph;

import java.io.OutputStream;

import com.datastax.driver.core.ResultSet;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.msc.research.cassandra.exception.RDFGraphProcessisngException;
import com.msc.research.cassandra.model.RDFTriple;

/**
 * Defines an API which is used to manipulate an RDF graph. For an example
 * builds an RDF graph given set of {@link RDFTriple} instances, traverse the
 * RDF graph model and prints it etc.
 * 
 * @author ravindra
 *
 */
public interface RDFGraphProcessingEngineService {
	/**
	 * Builds the RDF graph data model for a given set of {@link RDFTriple}
	 * instances.
	 * 
	 * @param resultSet
	 *            Set of RDf triples stored in the cassandra database.
	 * @throws RDFGraphProcessisngException
	 *             If an ERROR occurs during the RDF Graph processing.
	 */
	void build(final ResultSet resultSet) throws RDFGraphProcessisngException;

	/**
	 * Traverse the RDF graph model and prints it to the console.
	 * 
	 * @throws RDFGraphProcessisngException
	 *             If an ERROR occurs during the RDF Graph processing.
	 */
	void traverseAndPrint() throws RDFGraphProcessisngException;

	/**
	 * Serializes the RDF model to a given {@link OutputStream}
	 * 
	 * @param out
	 *            {@link OutputStream} to be written to.
	 * @throws RDFGraphProcessisngException
	 *             If an ERROR occurs during the RDF Graph processing.
	 */
	void serialize(final OutputStream out) throws RDFGraphProcessisngException;

	/**
	 * Queries the RDF Model and retrieves the answers.
	 * 
	 * @param pre
	 *            SPARQl query prefix
	 * @param qs
	 *            SPARQL query string value.
	 * @return A list of matching {@link RDFTriple} instances with the relevant
	 *         v
	 * @throws RDFGraphProcessisngException
	 *             If an ERROR occurs during the RDF Graph processing.
	 */
	String queryRdfModel(final String pre, final String qs)
			throws RDFGraphProcessisngException;

	/**
	 * Crestes RDF data model iterator for a given set of RDF data files in
	 * <code>NTriple</code> format. This is used to create the RDF store.
	 * 
	 * @param inputDataFiles
	 *            Input data files used to build the RDF store.
	 * @return An iterator for the data set, which represents the RDF store that
	 *         is about to build.
	 */
	StmtIterator getRDFDataSet(String[] inputDataFiles);

}
