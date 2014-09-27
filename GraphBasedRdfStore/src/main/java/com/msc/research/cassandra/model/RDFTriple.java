package com.msc.research.cassandra.model;

/**
 * This class represents an RDF Triple data model. An RDF Triple has <SUBJECT,
 * PREDICATE, OBJECT> form. This can be pronounced as SUBJECT has a PREDICATE
 * property whose value is the OBJECT.
 * 
 * @author ravindra
 *
 */
public class RDFTriple {

	private final String subject;
	private final String predicate;
	private final String object;

	/**
	 * Constructs an {@link RDFTriple} instance with the given data in it.
	 * 
	 * @param subject
	 *            The subject of the triple.
	 * @param predicate
	 *            The predicate value.
	 * @param object
	 *            The object value of the triple.
	 */
	public RDFTriple(String subject, String predicate, String object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	/**
	 * Fetches the <SUBJECT> value of this {@link RDFTriple} instance.
	 * 
	 * @return <SUBJECT> value of the current triple.
	 */
	public String getSubject() {
		return subject;
	}

	/**
	 * Fetches the <PREDICATE> value of this {@link RDFTriple} instance.
	 * 
	 * @return <PREDICATE> value of the current triple.
	 */
	public String getPredicate() {
		return predicate;
	}

	/**
	 * Fetches the <OBJECT> value of this {@link RDFTriple} instance.
	 * 
	 * @return <OBJECT> value of the current triple.
	 */
	public String getObject() {
		return object;
	}

}
