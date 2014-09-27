package com.msc.research.cassandra.graph;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
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
	private final String NS = "http://example.com/msc#";
	private Model model = null;
	private DataTransformer<List<Statement>, List<RDFTriple>> stmtToRDFTripleTransformer;

	private JenaRDFGraphProcessingEngine(
			final DataTransformer<List<Statement>, List<RDFTriple>> stmtToRDFTripleDataTransformer) {
		this.stmtToRDFTripleTransformer = stmtToRDFTripleDataTransformer;
	}

	@Override
	public void build(final List<RDFTriple> rdfTriples)
			throws RDFGraphProcessisngException {

		model = ModelFactory.createDefaultModel();
		model.setNsPrefix("eg", NS);
		for (RDFTriple rdfTriple : rdfTriples) {

			// Creates the resource first.
			Resource resource = model.createResource(NS
					+ rdfTriple.getSubject());
			// Then creates the property with it's predicate label.
			Property property = model.createProperty(NS
					+ rdfTriple.getPredicate());
			/*
			 * Finally add the property and its associated value to the
			 * resource.
			 */
			resource.addProperty(property, rdfTriple.getObject());
		}

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
	public List<RDFTriple> queryRdfModel(final RDFTriple triple)
			throws RDFGraphProcessisngException {
		if (model == null) {
			throw new RDFGraphProcessisngException(
					"A null RDF Grraph model can NOT be queried.");
		}
		List<Statement> statements = new ArrayList<Statement>();
		Resource subject = model.getResource(NS + triple.getSubject());
		Property predicate = model.getProperty(NS + triple.getPredicate());
		String object = triple.getObject();

		StmtIterator statementIter = model.listStatements(new SimpleSelector(
				subject, predicate, object));

		while (statementIter.hasNext()) {
			Statement statement = (Statement) statementIter.next();
			statements.add(statement);
		}

		return stmtToRDFTripleTransformer.transform(statements);
	}
}
