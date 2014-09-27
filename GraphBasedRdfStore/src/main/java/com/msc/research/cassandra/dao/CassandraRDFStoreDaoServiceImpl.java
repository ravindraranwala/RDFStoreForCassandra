package com.msc.research.cassandra.dao;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
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
	private void loadData(final List<RDFTriple> rdfTriples) {
		Session session = CassandraUtil.getSession();
		PreparedStatement statement = session
				.prepare("INSERT INTO rdfstore.tripletab "
						+ "(subject, predicate, object) " + "VALUES (?, ?, ?);");

		BoundStatement boundStatement = new BoundStatement(statement);

		for (RDFTriple rdfTriple : rdfTriples) {
			session.execute(boundStatement.bind(rdfTriple.getSubject(),
					rdfTriple.getPredicate(), rdfTriple.getObject()));
		}
	}

	@Override
	public List<RDFTriple> getRdfData() {
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
		return transformer.transform(resultSet);
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
		CassandraUtil.close();

	}

	@Override
	public void createRDFStore(List<RDFTriple> rdfTriples) {
		this.createSchema();
		this.loadData(rdfTriples);

	}

	@Override
	public void dropRDFStore() {
		CassandraUtil.getSession().execute("DROP KEYSPACE rdfstore;");

	}

	@Override
	public void printConnectionMetadata() {
		Metadata metadata = CassandraUtil.getCluster().getMetadata();

		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());

		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
	}
}
