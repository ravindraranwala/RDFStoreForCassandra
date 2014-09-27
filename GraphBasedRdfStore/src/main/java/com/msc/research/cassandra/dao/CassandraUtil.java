package com.msc.research.cassandra.dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Helper class providing static methods to perform common database operations
 * such as low-level cluster and session handling. By default localhost
 * (127.0.0.1) is considered as the node. If any one needs to create a session
 * for a different machine, she needs to set the node explicitly before creating
 * a session/cluster instance.
 * 
 * @author ravindra
 *
 */
public class CassandraUtil {
	private static Cluster cluster;
	private static Session session;
	private static String node;

	// static initializer block.
	static {
		node = "127.0.0.1";
		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect();
	}

	/**
	 * Sets the IP address of the current node in this cluster. By default the
	 * current node is set to 127.0.0.1. If anyone needs to set the node of this
	 * cluster to an address other than this, she needs to set the IP address
	 * explicitly.
	 * 
	 * @param node
	 *            IP address of the cluster node to be connected.
	 */
	public static void setNode(String node) {
		// overriding the default values.
		CassandraUtil.node = node;
		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect();
	}

	/**
	 * Fetches the current {@link Cluster} instance.
	 * 
	 * @return current {@link Cluster} instance.
	 */
	public static Cluster getCluster() {
		return cluster;
	}

	/**
	 * Fetches the {@link Session} instance associated with this {@link Cluster}
	 * 
	 * @return {@link Session} instance associated with this {@link Cluster}
	 */
	public static Session getSession() {
		return session;
	}

	/**
	 * Closes the current cluster and session instances.
	 */
	public static void close() {
		if (!cluster.isClosed() && !session.isClosed()) {
			cluster.close();
			session.close();
		}

	}

}
