package com.msc.research.cassandra.transformer;

/**
 * Responsible to transform the Source - S type into destination - D type.
 * 
 * @author ravindra
 *
 * @param <S>
 *            source type.
 * @param <D>
 *            destination type resulting after the successful transformation.
 */
public interface DataTransformer<S, D> {
	/**
	 * Transforms the Source - S into the Destination- D type.
	 * 
	 * @param source
	 *            source type which is subjected to the transformation process.
	 * @return Destination - D type after the transformation.
	 */
	public D transform(S source);

}
