package com.msc.research.cassandra.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.msc.research.cassandra.client.ConsoleClient;
import com.msc.research.cassandra.dao.CassandraRDFStoreDaoServiceImpl;
import com.msc.research.cassandra.dao.RDFStoreDaoService;
import com.msc.research.cassandra.exception.RDFGraphProcessisngException;
import com.msc.research.cassandra.graph.JenaRDFGraphProcessingEngine;
import com.msc.research.cassandra.graph.RDFGraphProcessingEngineService;
import com.msc.research.cassandra.model.RDFTriple;

/**
 * Controls and manages the business work-flow related to an RDF store.
 * 
 * @author ravindra
 *
 */
public class RDFStoreController {

	public static void main(String[] args) throws RDFGraphProcessisngException,
			IOException {
		List<RDFTriple> rdfTriples = createRDFData();
		RDFGraphProcessingEngineService rdfGraphProcessingEngineService = JenaRDFGraphProcessingEngine
				.newInstance();
		RDFStoreDaoService rdfStoreDaoService = null;

		try {
			rdfStoreDaoService = CassandraRDFStoreDaoServiceImpl.newInstance();
			// Prints the metadata related to this connection.
			rdfStoreDaoService.printConnectionMetadata();

			// Drop the existing RDF store first.
			// rdfStoreDaoService.dropRDFStore();

			// Then create a new RDF Store with some sample data in it.
			// rdfStoreDaoService.createRDFStore(rdfTriples);

			// read RDF triples from the RDF store.
			rdfTriples = rdfStoreDaoService.getRdfData();

		} finally {
			/*
			 * when the interaction with the DAO layer is completed, merely
			 * close the connection.
			 */
			rdfStoreDaoService.close();
		}

		// Then build the RDF graph model.
		rdfGraphProcessingEngineService.build(rdfTriples);

		// Finally write the RDF model built, to the console.
		// rdfGraphProcessingEngineService.serialize(System.out);
		// rdfGraphProcessingEngineService.traverseAndPrint();

		// Query the RDF model.
		ConsoleClient consoleClient = new ConsoleClient();
		String[] userInput = consoleClient.readInput();

		// Create the RDFTriple from the user input.
		RDFTriple input = createRDFTripleFromUserInput(userInput);
		List<RDFTriple> queryResult = rdfGraphProcessingEngineService
				.queryRdfModel(input);
		List<String> resultStr = new ArrayList<String>(queryResult.size());
		for (RDFTriple rdfTriple : queryResult) {
			resultStr.add(rdfTriple.getObject());
		}

		// renders the result to the user.
		consoleClient.renderResult(resultStr);
		// Find all the films directed by J.Cameron.
		// RDFTriple triple = new RDFTriple("J_Cameron", "directs", null);
		// List<RDFTriple> queryResult = rdfGraphProcessingEngineService
		// .queryRdfModel(triple);
		// System.out.println("The Films directed by J_Cameron: ");
		// for (RDFTriple rdfTriple : queryResult) {
		// System.out.println(rdfTriple.getObject());
		// }

	}

	private static List<RDFTriple> createRDFData() {
		// Setting up some dummy/sample triples to be loaded into the RDF store.
		List<RDFTriple> rdfTriples = new ArrayList<RDFTriple>();
		rdfTriples.add(new RDFTriple("P_Haggis", "directs", "Crash"));
		rdfTriples.add(new RDFTriple("Crash", "casts", "D_Cheadle"));
		rdfTriples.add(new RDFTriple("Crash", "has_award", "Best_Picture"));
		rdfTriples.add(new RDFTriple("J_Cameron", "directs", "Titanic"));
		rdfTriples.add(new RDFTriple("J_Cameron", "directs", "Avatar"));
		rdfTriples.add(new RDFTriple("J_Cameron", "wins", "Oscar_Award"));
		rdfTriples.add(new RDFTriple("Titanic", "has_award", "Best_Picture"));
		rdfTriples.add(new RDFTriple("Titanic", "casts", "L_DiCaprio"));
		rdfTriples.add(new RDFTriple("Avatar", "casts", "S_Worthington"));
		rdfTriples.add(new RDFTriple("G_Lucas", "wins", "Saturn-Award"));
		rdfTriples.add(new RDFTriple("G_Lucas", "writes", "Star War VI"));
		rdfTriples.add(new RDFTriple("Star War VI", "casts", "M_Hamill"));

		return rdfTriples;
	}

	private static RDFTriple createRDFTripleFromUserInput(final String[] input) {
		String subject = null;
		String predicate = null;
		String object = null;
		if (!input[0].matches("\\?")) {
			subject = input[0];
		}
		if (!input[1].matches("\\?")) {
			predicate = input[1];
		}
		if (!input[2].matches("\\?")) {
			object = input[2];
		}

		return new RDFTriple(subject, predicate, object);
	}

}
