package com.msc.research.cassandra.graph;

import java.io.OutputStream;
import java.util.List;

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
	 * @param rdfTriples
	 *            Set of {@link RDFTriple} instances used to build the in-memory
	 *            RDF graph model.
	 * @throws RDFGraphProcessisngException
	 *             If an ERROR occurs during the RDF Graph processing.
	 */
	void build(final List<RDFTriple> rdfTriples)
			throws RDFGraphProcessisngException;

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
	 * @param triple
	 *            A triple instance with optional subject, predicate and object
	 *            values. NULL value for each of them matches with anything.
	 * @return A list of matching {@link RDFTriple} instances with the relevant
	 *         data in it.
	 * @throws RDFGraphProcessisngException
	 *             If an ERROR occurs during the RDF Graph processing.
	 */
	List<RDFTriple> queryRdfModel(final RDFTriple triple)
			throws RDFGraphProcessisngException;

}
