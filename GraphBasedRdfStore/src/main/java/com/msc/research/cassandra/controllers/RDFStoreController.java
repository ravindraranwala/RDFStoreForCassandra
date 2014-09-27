package com.msc.research.cassandra.controllers;

import java.io.IOException;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.datastax.driver.core.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.msc.research.cassandra.dao.CassandraRDFStoreDaoServiceImpl;
import com.msc.research.cassandra.dao.RDFStoreDaoService;
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

		final String pre = StrUtils.strjoinNL(
				"PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>",
				"PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>");

		final String qs = StrUtils.strjoinNL("SELECT ?s ?homepage WHERE {",
				"<http://dbpedia.org/resource/Berlin> geo:lat ?berlinLat .",
				"<http://dbpedia.org/resource/Berlin> geo:long ?berlinLong .",
				"?s geo:lat ?lat .", "?s geo:long ?long .",
				"?s foaf:homepage ?homepage .", "FILTER (",
				"?lat        <=     ?berlinLat + 0.03190235436 &&",
				"?long       >=     ?berlinLong - 0.08679199218 &&",
				"?lat        >=     ?berlinLat - 0.03190235436 && ",
				"?long       <=     ?berlinLong + 0.08679199218)", "}");

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

		rdfGraphProcessingEngineService.queryRdfModel(pre, qs);
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

		// print out the predicate, subject and object of each statement
		// while (iter.hasNext()) {
		// Statement stmt = iter.nextStatement(); // get next statement
		// Resource subject = stmt.getSubject(); // get the subject
		// Property predicate = stmt.getPredicate(); // get the predicate
		// RDFNode object = stmt.getObject(); // get the object
		//
		// rdfTriples.add(new RDFTriple(subject.toString(), predicate
		// .toString(), object.toString()));
		// }
		// rdfTriples.add(new RDFTriple("P_Haggis", "directs", "Crash"));
		// rdfTriples.add(new RDFTriple("Crash", "casts", "D_Cheadle"));
		// rdfTriples.add(new RDFTriple("Crash", "has_award", "Best_Picture"));
		// rdfTriples.add(new RDFTriple("J_Cameron", "directs", "Titanic"));
		// rdfTriples.add(new RDFTriple("J_Cameron", "directs", "Avatar"));
		// rdfTriples.add(new RDFTriple("J_Cameron", "wins", "Oscar_Award"));
		// rdfTriples.add(new RDFTriple("Titanic", "has_award",
		// "Best_Picture"));
		// rdfTriples.add(new RDFTriple("Titanic", "casts", "L_DiCaprio"));
		// rdfTriples.add(new RDFTriple("Avatar", "casts", "S_Worthington"));
		// rdfTriples.add(new RDFTriple("G_Lucas", "wins", "Saturn-Award"));
		// rdfTriples.add(new RDFTriple("G_Lucas", "writes", "Star War VI"));
		// rdfTriples.add(new RDFTriple("Star War VI", "casts", "M_Hamill"));

		// return rdfTriples;
	}

}
