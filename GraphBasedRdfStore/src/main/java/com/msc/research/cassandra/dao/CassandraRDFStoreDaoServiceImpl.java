package com.msc.research.cassandra.dao;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.msc.research.cassandra.model.RDFTriple;
import com.msc.research.cassandra.transformer.DataTransformer;
import com.msc.research.cassandra.transformer.ResultSetToRDFTripleTransformer;

/**
 * This class is used to build a simple triple store with some sample data in
 * its triple table. The triple store is built on top of Apache Cassandra. Also
 * this class encapsulates all the persistance logic used to interact with
 * Apache Cassandra. This accts as an Apache Cassandra facade which hides the
 * complexity of dealing with cassandra (for an example, CONNECT, CREATE, READ,
 * WRITE etc.) from the end users of this API.
 * 
 * @author ravindra
 *
 */
public class CassandraRDFStoreDaoServiceImpl implements RDFStoreDaoService {
	private static final Logger LOGGER = Logger
			.getLogger(CassandraRDFStoreDaoServiceImpl.class);
	private final DataTransformer<ResultSet, List<RDFTriple>> transformer;

	private static RDFStoreDaoService rdfStoreDaoService = null;

	private CassandraRDFStoreDaoServiceImpl(
			DataTransformer<ResultSet, List<RDFTriple>> transformer) {
		this.transformer = transformer;
	}

	/*
	 * Creates the Apache Cassandra Keyspace and the triple-table to store the
	 * RDF triples.
	 */
	private void createSchema() {
		LOGGER.info("Creating the Database schema.");
		Session session = CassandraUtil.getSession();
		session.execute("CREATE KEYSPACE rdfstore WITH replication"
				+ "= {'class':'SimpleStrategy', 'replication_factor':3};");

		session.execute("CREATE TABLE  rdfstore.tripletab(" + "subject text,"
				+ "predicate text," + "object text,"
				+ "PRIMARY KEY (subject, predicate, object)" + ");");
	}

	/*
	 * Loads the RDF data into the triple table.
	 */
	private void loadData(final StmtIterator stmtIterator) {
		LOGGER.info("Loading the data into the database.");
		Session session = CassandraUtil.getSession();
		PreparedStatement statement = session
				.prepare("INSERT INTO rdfstore.tripletab "
						+ "(subject, predicate, object) " + "VALUES (?, ?, ?);");

		BoundStatement boundStatement = new BoundStatement(statement);

		while (stmtIterator.hasNext()) {
			Statement stmt = (Statement) stmtIterator.next();
			session.execute(boundStatement
					.bind(stmt.getSubject().toString(), stmt.getPredicate()
							.toString(), stmt.getObject().toString()));
		}

	}

	@Override
	public ResultSet getRdfData() {
		LOGGER.info("Fetching data from the RDF store.");
		ResultSet resultSet = CassandraUtil.getSession().execute(
				"SELECT * FROM rdfstore.tripletab;");

		// System.out
		// .println(String
		// .format("%-30s\t%-20s\t%-20s\n%s", "subject",
		// "predicate", "object",
		// "-------------------------------+-----------------------+--------------------"));
		//
		// for (Row row : resultSet) {
		// System.out.println(String.format("%-30s\t%-20s\t%-20s",
		// row.getString("subject"), row.getString("predicate"),
		// row.getString("object")));
		//
		// }
		//
		// System.out.println("\n");
		return resultSet;
	}

	/**
	 * Creates a Singleton instance of this DAO implementation.
	 * 
	 * @return A singleton instance of {@link CassandraRDFStoreDaoServiceImpl}
	 */
	public static RDFStoreDaoService newInstance() {
		if (rdfStoreDaoService == null) {
			rdfStoreDaoService = new CassandraRDFStoreDaoServiceImpl(
					new ResultSetToRDFTripleTransformer());
		}
		return rdfStoreDaoService;
	}

	@Override
	public void close() {
		LOGGER.info("Closing the Database connection established.");
		CassandraUtil.close();

	}

	@Override
	public void createRDFStore(final StmtIterator stmtIterator) {
		this.createSchema();
		this.loadData(stmtIterator);

	}

	@Override
	public void dropRDFStore() {
		close();
		LOGGER.info("Dropping the RDf Store.");
		CassandraUtil.getSession().execute("DROP KEYSPACE IF EXISTS rdfstore;");

	}

	@Override
	public void printConnectionMetadata() {
		Metadata metadata = CassandraUtil.getCluster().getMetadata();

		LOGGER.info("Connected to cluster: " + metadata.getClusterName());

		for (Host host : metadata.getAllHosts()) {
			LOGGER.info("Datacenter: " + host.getDatacenter() + "; Host: "
					+ host.getAddress() + "; Rack: " + host.getRack());
		}
	}
}
