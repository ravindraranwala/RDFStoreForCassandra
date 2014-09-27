package com.msc.research.cassandra.graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.logging.Logger;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.msc.research.cassandra.exception.RDFGraphProcessisngException;
import com.msc.research.cassandra.model.RDFTriple;
import com.msc.research.cassandra.transformer.DataTransformer;
import com.msc.research.cassandra.transformer.StatementToRDFTripleTransformer;

/**
 * Apache Jena implementation of the Graph Processisng API.
 * 
 * @author ravindra
 *
 */
public class JenaRDFGraphProcessingEngine implements
		RDFGraphProcessingEngineService {
	private static RDFGraphProcessingEngineService rdfGraphProcessingEngine;
	// private final String NS = "http://example.com/msc#";
	private Model model = null;

	private static final Logger logger = Logger
			.getLogger(JenaRDFGraphProcessingEngine.class.getName());

	private JenaRDFGraphProcessingEngine(
			final DataTransformer<List<Statement>, List<RDFTriple>> stmtToRDFTripleDataTransformer) {
	}

	@Override
	public void build(final ResultSet source)
			throws RDFGraphProcessisngException {

		model = ModelFactory.createDefaultModel();
		logger.info("Building the Graph Model from the data fetched fromCassandra cluster.");
		for (Row row : source) {
			Resource resource = model.createResource(row.getString("subject"));
			Property property = model
					.createProperty(row.getString("predicate"));

			final String strObjValue = row.getString("object");
			final String splitterSymbol = "^^";
			int bound = strObjValue.indexOf("^^");

			// This is the default value.
			RDFNode objectNode = model.createResource(row.getString("object"));

			if (bound > 0) {
				String numericValue = strObjValue.substring(0, bound);

				// Population can NOT be a double, it should be a long value.
				// So, merely omit it.
				if (isDouble(numericValue)
						&& !row.getString("predicate").contains(
								"http://www.geonames.org/ontology#population")) {
					objectNode = model.createTypedLiteral(new Double(
							numericValue));
				} else if (isLong(numericValue)) {
					if (numericValue.indexOf(",") >= 0) {
						numericValue = new StringBuffer(numericValue)
								.deleteCharAt(numericValue.indexOf(","))
								.toString();
					}
					objectNode = model
							.createTypedLiteral(new Long(numericValue));
				}

			}

			// Literal typedLiteral = model.createTypedLiteral(new Integer(25));
			// logger.info(typedLiteral.getValue().toString());
			// Resource object = model.createResource(row.getString("object"));

			// Statement stmt = model.createStatement(resource, property,
			// typedLiteral.getValue().toString());
			// model.add(stmt);

			resource.addProperty(property, objectNode);
		}

		// String inputFileName =
		// "/home/ravindra/msc/Year2/msc-research/SPARQL-benchmarking-tool/db-pedia/homepages-fixed.nt";
		// model = RDFDataMgr.loadModel(inputFileName, Lang.NTRIPLES);
		//
		// inputFileName =
		// "/home/ravindra/msc/Year2/msc-research/SPARQL-benchmarking-tool/db-pedia/geocoordinates-fixed.nt";
		// Model geocoordinateModel = RDFDataMgr.loadModel(inputFileName);
		// model.add(geocoordinateModel);

		logger.info("Graph Model is built successfully.");
		// logger.info("Printing the Graph Model.");

		FileOutputStream fop = null;
		File file = new File("test-rdf.txt");
		try {
			fop = new FileOutputStream(file);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		RDFDataMgr.write(fop, model, Lang.NTRIPLES);
		// RDFDataMgr.write(System.out, model, Lang.NTRIPLES);
		// model.add(iter)
		// for (RDFTriple rdfTriple : rdfTriples) {
		//
		// // Creates the resource first.
		// Resource resource = model.createResource(rdfTriple.getSubject());
		// // Then creates the property with it's predicate label.
		// Property property = model.createProperty(rdfTriple.getPredicate());
		// /*
		// * Finally add the property and its associated value to the
		// * resource.
		// */
		// resource.addProperty(property, rdfTriple.getObject());
		// }

	}

	public static boolean isLong(String s) {
		try {
			if (s.contains(",")) {
				s = new StringBuffer(s).deleteCharAt(s.indexOf(",")).toString();
			}
			Long.parseLong(s);
		} catch (NumberFormatException e) {
			return false;
		}
		// only got here if we didn't return false
		return true;
	}

	public static boolean isDouble(String s) {
		try {
			Double.parseDouble(s);
		} catch (NumberFormatException e) {
			return false;
		}

		return true;
	}

	@Override
	public void traverseAndPrint() throws RDFGraphProcessisngException {
		if (model == null) {
			throw new RDFGraphProcessisngException(
					"A null RDF Graph model can NOT be traversed.");
		}
		// list the statements in the Model
		StmtIterator iter = model.listStatements();

		System.out.println("*** Printing RDF Graph Model. ***");
		// print out the predicate, subject and object of each statement
		while (iter.hasNext()) {
			Statement stmt = iter.nextStatement(); // get next statement
			Resource subject = stmt.getSubject(); // get the subject
			Property predicate = stmt.getPredicate(); // get the predicate
			RDFNode object = stmt.getObject(); // get the object

			System.out.print(subject.toString());
			System.out.print(" " + predicate.toString() + " ");
			if (object instanceof Resource) {
				System.out.print(object.toString());
			} else {
				// object is a literal
				System.out.print(" \"" + object.toString() + "\"");
			}

			System.out.println(" .");
		}

	}

	/**
	 * Creates a singleton instance of {@link JenaRDFGraphProcessingEngine}
	 * class.
	 * 
	 * @return A singleton instance of the {@link JenaRDFGraphProcessingEngine}
	 *         class.
	 */
	public static RDFGraphProcessingEngineService newInstance() {
		if (rdfGraphProcessingEngine == null) {
			rdfGraphProcessingEngine = new JenaRDFGraphProcessingEngine(
					new StatementToRDFTripleTransformer());
		}

		return rdfGraphProcessingEngine;
	}

	@Override
	public void serialize(final OutputStream out)
			throws RDFGraphProcessisngException {
		if (model == null) {
			throw new RDFGraphProcessisngException(
					"A null RDFGraph model can NOT be serialized.");
		}
		model.write(out, "RDF/XML");

	}

	@Override
	public void queryRdfModel(final String pre, final String qs)
			throws RDFGraphProcessisngException {
		if (model == null) {
			throw new RDFGraphProcessisngException(
					"A null RDF Grraph model can NOT be queried.");
		}

		logger.info("Querying the RDF graph model.");
		Query query = QueryFactory.create(pre + qs);
		try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
			com.hp.hpl.jena.query.ResultSet results = qexec.execSelect();
			ResultSetFormatter.out(System.out, results);
		}
	}

}
