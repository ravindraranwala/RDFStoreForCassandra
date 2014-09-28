package com.msc.research.cassandra.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;

public class BerlinBenchmarkTest {

	public static void main(String[] args) {
		String inputFileName = "/home/ravindra/msc/Year2/msc-research/SPARQL-benchmarking-tool/db-pedia/homepages-fixed.nt";
		Model model = RDFDataMgr.loadModel(inputFileName, Lang.NTRIPLES);

		inputFileName = "/home/ravindra/msc/Year2/msc-research/SPARQL-benchmarking-tool/db-pedia/geocoordinates-fixed.nt";
		Model geocoordinateModel = RDFDataMgr.loadModel(inputFileName);
		model.add(geocoordinateModel);

		// StmtIterator iter = model.listStatements();
		// System.out.println("*** Printing RDF Graph Model. ***");
		// // print out the predicate, subject and object of each statement
		// while (iter.hasNext()) {
		// Statement stmt = iter.nextStatement(); // get next statement
		// Resource subject = stmt.getSubject(); // get the subject
		// Property predicate = stmt.getPredicate(); // get the predicate
		// RDFNode object = stmt.getObject(); // get the object
		//
		// System.out.print(subject.toString());
		// System.out.print(" " + predicate.toString() + " ");
		// if (object instanceof Resource) {
		// System.out.print(object.toString());
		// } else {
		// // object is a literal
		// System.out.print(" \"" + object.toString() + "\"");
		// }
		//
		// System.out.println(" .");
		// }

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

		queryModel(model);
		// model.write(System.out, "N-TRIPLES");
	}

	private static void queryModel(Model model) {

		// final String qs = StrUtils
		// .strjoinNL(
		// "SELECT ?p ?o WHERE {",
		// "<http://dbpedia.org/resource/Metropolitan_Museum_of_Art> ?p ?o",
		// "}");

		final String pre = StrUtils
				.strjoinNL("PREFIX p: <http://dbpedia.org/resource/>");
		final String qs = StrUtils.strjoinNL("SELECT ?p ?o WHERE {",
				"p:Metropolitan_Museum_of_Art ?p ?o", "}");

		// final String pre = StrUtils
		// .strjoinNL("PREFIX p: <http://dbpedia.org/resource/>");
		//
		// final String qs = StrUtils.strjoinNL("SELECT ?p ?o", "WHERE {",
		// "p:Kevin_Bacon  ?p ?o", "}");

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

		Query query = QueryFactory.create(pre + qs);
		try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
			ResultSet results = qexec.execSelect();
			ResultSetFormatter.out(System.out, results);
		}
	}
}
