package com.msc.research.cassandra.exception;

/**
 * Thrown to indicate that an ERROR occurred while RDF Graph processing.
 * 
 * @author ravindra
 *
 */
public class RDFGraphProcessisngException extends Exception {
	private final String message;

	/**
	 * Constructs a new instance of {@link RDFGraphProcessisngException} with
	 * the specified detailed message.
	 * 
	 * @param message
	 */
	public RDFGraphProcessisngException(String message) {
		super();
		this.message = message;
	}

	/**
	 * Returns the ERROR message related to the Graph processing.
	 */
	public String getMessage() {
		return message;
	};

}
