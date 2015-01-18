package com.msc.research.cassandra.controllers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.datastax.driver.core.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.msc.research.cassandra.dao.CassandraRDFStoreDaoServiceImpl;
import com.msc.research.cassandra.dao.RDFStoreDaoService;
import com.msc.research.cassandra.exception.InvalidChoiceException;
import com.msc.research.cassandra.exception.RDFGraphProcessisngException;
import com.msc.research.cassandra.graph.JenaRDFGraphProcessingEngine;
import com.msc.research.cassandra.graph.RDFGraphProcessingEngineService;

/**
 * Controls and manages the business work-flow related to an RDF store.
 * 
 * @author ravindra
 *
 */
public class RDFStoreController {

	public static void main(String[] args) throws RDFGraphProcessisngException,
			IOException {

		RDFGraphProcessingEngineService rdfGraphProcessingEngineService = JenaRDFGraphProcessingEngine
				.newInstance();
		RDFStoreDaoService rdfStoreDaoService = null;

		ResultSet resultSet = null;

		try {
			rdfStoreDaoService = CassandraRDFStoreDaoServiceImpl.newInstance();
			// Prints the metadata related to this connection.
			// rdfStoreDaoService.printConnectionMetadata();

			// Drop the existing RDF store first.
			// rdfStoreDaoService.dropRDFStore();

			// Then create a new RDF Store with some sample data in it.
			// rdfStoreDaoService.createRDFStore(createRDFData());

			// read RDF triples from the RDF store.
			resultSet = rdfStoreDaoService.getRdfData();

			// Then build the RDF graph model.
			rdfGraphProcessingEngineService.build(resultSet);

		} finally {
			/*
			 * when the interaction with the DAO layer is completed, merely
			 * close the connection.
			 */
			rdfStoreDaoService.close();
		}

		// Finally write the RDF model built, to the console.
		// rdfGraphProcessingEngineService.serialize(System.out);
		// rdfGraphProcessingEngineService.traverseAndPrint();

		// final String pre = StrUtils
		// .strjoinNL("PREFIX p: <http://dbpedia.org/resource/>");
		//
		// final String qs = StrUtils.strjoinNL("SELECT ?p ?o", "WHERE {",
		// "p:Kevin_Bacon  ?p ?o", "}");

		/*
		 * Homepages of resources roughly in the area of Berlin.
		 */

		// final String pre = StrUtils.strjoinNL(
		// "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>",
		// "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
		// "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>");
		//
		// final String qs = StrUtils.strjoinNL("SELECT ?s ?homepage WHERE {",
		// "<http://dbpedia.org/resource/Berlin> geo:lat ?berlinLat .",
		// "<http://dbpedia.org/resource/Berlin> geo:long ?berlinLong .",
		// "?s geo:lat ?lat .", "?s geo:long ?long .",
		// "?s foaf:homepage ?homepage .", "FILTER (",
		// "?lat        <=     ?berlinLat + 0.03190235436 &&",
		// "?long       >=     ?berlinLong - 0.08679199218 &&",
		// "?lat        >=     ?berlinLat - 0.03190235436 && ",
		// "?long       <=     ?berlinLong + 0.08679199218)", "}");

		/*
		 * Homepages of resources roughly in the area of New York City
		 */
		// final String pre = StrUtils.strjoinNL(
		// "PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>",
		// "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
		// "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>",
		// "PREFIX p: <http://dbpedia.org/property/>");
		//
		// final String qs = StrUtils
		// .strjoinNL(
		// "SELECT ?s ?homepage WHERE {",
		// "<http://dbpedia.org/resource/New_York_City> geo:lat ?nyLat .",
		// "<http://dbpedia.org/resource/New_York_City> geo:long ?nyLong . ",
		// "?s geo:lat ?lat .", "?s geo:long ?long .",
		// "?s foaf:homepage ?homepage .", "FILTER (",
		// "?lat        <=     ?nyLat + 0.3190235436 &&",
		// "?long       >=     ?nyLong - 0.8679199218 &&",
		// "?lat        >=     ?nyLat - 0.3190235436 && ",
		// "?long       <=     ?nyLong + 0.8679199218)", "}");

		// rdfGraphProcessingEngineService.queryRdfModel(pre, qs);

		processUserInput(rdfGraphProcessingEngineService);

		// Find all the films directed by J.Cameron.
		// RDFTriple triple = new RDFTriple("J_Cameron", "directs", null);
		// List<RDFTriple> queryResult = rdfGraphProcessingEngineService
		// .queryRdfModel(triple);
		// System.out.println("The Films directed by J_Cameron: ");
		// for (RDFTriple rdfTriple : queryResult) {
		// System.out.println(rdfTriple.getObject());
		// }

	}

	private static StmtIterator createRDFData() {
		// Setting up some dummy/sample triples to be loaded into the RDF store.
		String inputFileName = "/home/ravindra/msc/Year2/msc-research/SPARQL-benchmarking-tool/db-pedia/homepages-fixed.nt";
		Model model = RDFDataMgr.loadModel(inputFileName, Lang.NTRIPLES);

		inputFileName = "/home/ravindra/msc/Year2/msc-research/SPARQL-benchmarking-tool/db-pedia/geocoordinates-fixed.nt";
		Model geocoordinateModel = RDFDataMgr.loadModel(inputFileName);
		model.add(geocoordinateModel);

		return model.listStatements();
	}

	private static String readInput() throws IOException {
		// open up standard input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		return br.readLine();
	}

	private static void processUserInput(
			RDFGraphProcessingEngineService rdfGraphProcessingEngineService) {
		// Prompt the user first.
		System.out
				.println("***********************************************************************");
		System.out.println("You have FOUR queries to execute.");
		System.out
				.println("To Get infor about  Metropolitan_Museum_of_Art:- ENTER 1");
		System.out.println("To Get infor about Kevin Bacon:- ENTER 2");
		System.out
				.println("To Get Homepages of resources roughly in the area of Berlin:- ENTER 3");
		System.out
				.println("To Get Homepages of resources roughly in the area of New York City: ENTER 4");
		System.out.println("To QUIT: ENTER 5");
		System.out
				.println("***********************************************************************");

		while (true) {
			System.out.println("Enter a number between <1-4> or 5 to Quit");
			// read the input given by the user.
			try {
				String userInput = readInput();
				int choice = Integer.parseInt(userInput);
				String[] sparqlQuery = generateSPARQLQuery(choice);

				long startTime = System.currentTimeMillis();

				// Executing the query against the RDF model.
				String result = rdfGraphProcessingEngineService.queryRdfModel(
						sparqlQuery[0], sparqlQuery[1]);

				System.out.println(result);

				long stopTime = System.currentTimeMillis();
				long elapsedTime = stopTime - startTime;
				System.out.println(elapsedTime);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidChoiceException e) {
				System.out.println("Bye !");
				// TODO Auto-generated catch block
				// e.printStackTrace();
				break;
			} catch (RDFGraphProcessisngException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static String[] generateSPARQLQuery(final int choice)
			throws InvalidChoiceException {
		String pre = null;
		String qs = null;
		if (choice == 1) {
			pre = StrUtils
					.strjoinNL("PREFIX p: <http://dbpedia.org/resource/>");
			qs = StrUtils.strjoinNL("SELECT ?p ?o WHERE {",
					"p:Metropolitan_Museum_of_Art ?p ?o", "}");
		} else if (choice == 2) {
			pre = StrUtils
					.strjoinNL("PREFIX p: <http://dbpedia.org/resource/>");

			qs = StrUtils.strjoinNL("SELECT ?p ?o", "WHERE {",
					"p:Kevin_Bacon  ?p ?o", "}");
		} else if (choice == 3) {
			/*
			 * Homepages of resources roughly in the area of Berlin.
			 */

			pre = StrUtils.strjoinNL(
					"PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>",
					"PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>");

			qs = StrUtils
					.strjoinNL(
							"SELECT ?s ?homepage WHERE {",
							"<http://dbpedia.org/resource/Berlin> geo:lat ?berlinLat .",
							"<http://dbpedia.org/resource/Berlin> geo:long ?berlinLong .",
							"?s geo:lat ?lat .",
							"?s geo:long ?long .",
							"?s foaf:homepage ?homepage .",
							"FILTER (",
							"?lat        <=     ?berlinLat + 0.03190235436 &&",
							"?long       >=     ?berlinLong - 0.08679199218 &&",
							"?lat        >=     ?berlinLat - 0.03190235436 && ",
							"?long       <=     ?berlinLong + 0.08679199218)",
							"}");

		} else if (choice == 4) {
			pre = StrUtils.strjoinNL(
					"PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>",
					"PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>",
					"PREFIX p: <http://dbpedia.org/property/>");

			qs = StrUtils
					.strjoinNL(
							"SELECT ?s ?homepage WHERE {",
							"<http://dbpedia.org/resource/New_York_City> geo:lat ?nyLat .",
							"<http://dbpedia.org/resource/New_York_City> geo:long ?nyLong . ",
							"?s geo:lat ?lat .", "?s geo:long ?long .",
							"?s foaf:homepage ?homepage .", "FILTER (",
							"?lat        <=     ?nyLat + 0.3190235436 &&",
							"?long       >=     ?nyLong - 0.8679199218 &&",
							"?lat        >=     ?nyLat - 0.3190235436 && ",
							"?long       <=     ?nyLong + 0.8679199218)", "}");
		} else {
			throw new InvalidChoiceException();
		}
		return new String[] { pre, qs };
	}

}
