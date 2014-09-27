package com.msc.research.cassandra.dao;

import com.datastax.driver.core.ResultSet;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * This interface defines all the necessary methods to build an RDF store and
 * query it. It also provides some utility methods to connect/disconnect to the
 * database on which the Triple store is placed. This class is responsible for
 * performing all the CRUD operations related to the triple table.
 * 
 * @author ravindra
 *
 */
public interface RDFStoreDaoService {

	/**
	 * Closes the connection established with the database/file.
	 */
	void close();

	/**
	 * Creates the database schema necessary to build the triple store and loads
	 * the RDF data into it. The triple store usually contains a triple table
	 * which contains SUBJECT, PREDICATE and OBJECT as columns in it.
	 * 
	 * @param stmtIterator
	 */
	void createRDFStore(final StmtIterator stmtIterator);

	/**
	 * Queries the RDF store and fetches all the triples stored in it.
	 * 
	 * @return A {@link ResultSet} instance which is backed by the RDF data in
	 *         the cassandra cluster.
	 */
	ResultSet getRdfData();

	/**
	 * Merely drops the RDF store created.
	 */
	void dropRDFStore();

	/**
	 * Prints the metadata associated with this connection.
	 */
	void printConnectionMetadata();
}
