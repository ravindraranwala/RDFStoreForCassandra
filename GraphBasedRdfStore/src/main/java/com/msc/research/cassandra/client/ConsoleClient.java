package com.msc.research.cassandra.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Interacts with the end users. Takes the user input and renders the result to
 * the end users.
 * 
 * @author ravindra
 *
 */
public class ConsoleClient {
	/**
	 * Prompts the user and reads the input.
	 * 
	 * @return
	 * @throws IOException
	 */
	public String[] readInput() throws IOException {
		System.out
				.println("Enter subject:predicate:object values seperated by ':', unknows values should be substituted with '?'");
		// open up standard input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		return br.readLine().split(":");
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

}
