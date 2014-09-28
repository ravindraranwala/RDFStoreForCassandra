package com.msc.research.cassandra.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.jena.atlas.lib.StrUtils;

import com.msc.research.cassandra.controllers.RDFStoreController;
import com.msc.research.cassandra.exception.InvalidChoiceException;

/**
 * Interacts with the end users. Takes the user input and renders the result to
 * the end users.
 * 
 * @author ravindra
 *
 */
public class ConsoleClient implements Runnable {

	private final RDFStoreController rdfStoreController;

	public ConsoleClient(RDFStoreController rdfStoreController) {
		this.rdfStoreController = rdfStoreController;
	}

	/**
	 * Prompts the user and reads the input.
	 * 
	 * @return
	 * @throws IOException
	 */
	public String readInput() throws IOException {
		// open up standard input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		return br.readLine();
	}

	/**
	 * Renders the result to the user query.
	 * 
	 * @param results
	 *            The result to be rendered.
	 */
	public void renderResult(List<String> results) {
		for (String result : results) {
			System.out.println(result);
		}
	}

	@Override
	public void run() {
		// Prompt the user first.
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
			// read the input given by the user.
			try {
				String userInput = readInput();
				int choice = Integer.parseInt(userInput);
				String[] sparqlQuery = generateSPARQLQuery(choice);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidChoiceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				break;
			}
		}
	}

	private String[] generateSPARQLQuery(final int choice)
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
