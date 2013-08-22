/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

/**
 * Demonstrates triangle closing in simple, unweighted graphs for Giraph. Triangle Closing: Vertex A and B maintain
 * out-edges to C and D The algorithm, when finished, populates all vertices' value with an array of Writables
 * representing all the vertices that each should form an out-edge to (connect with, if this is a social graph.) In this
 * example, vertices A and B would hold empty arrays since they are already connected with C and D. Results: If the
 * graph is undirected, C would hold value, D and D would hold value C, since both are neighbors of A and B and yet both
 * were not previously connected to each other. In a social graph, the result values for vertex X would represent people
 * that are likely a part of a person X's social circle (they know one or more people X is connected to already) but X
 * had not previously met them yet. Given this new information, X can decide to connect to vertices (peoople) in the
 * result array or not. Results at each vertex are ordered in terms of the # of neighbors who are connected to each
 * vertex listed in the final vertex value. The more of a vertex's neighbors who "know" someone, the stronger your
 * social relationship is presumed to be to that vertex (assuming a social graph) and the more likely you should connect
 * with them. In this implementation, Edge Values are not used, but could be adapted to represent additional qualities
 * that could affect the ordering of the final result array.
 */
public class SimpleTriangleClosingComputation extends
		BasicComputation<IntWritable, SimpleTriangleClosingComputation.IntArrayListWritable, NullWritable, IntWritable> {

	/** Vertices to close the triangle, ranked by frequency of in-msgs */
	private final Map<Integer, Integer> closeMap = Maps.<Integer, Integer> newHashMap();

	@Override
	public void compute(final Vertex<IntWritable, IntArrayListWritable, NullWritable> vertex,
			final Iterable<IntWritable> messages) throws IOException {
		if (getSuperstep() == 0) {
			// send list of this vertex's neighbors to all neighbors
			for (final Edge<IntWritable, NullWritable> edge : vertex.getEdges()) {
				sendMessageToAllEdges(vertex, edge.getTargetVertexId());
			}
		} else {
			final IntArrayListWritable outputList = new IntArrayListWritable();
			for (final IntWritable message : messages) {
				final int intMsg = message.get();
				final int current = closeMap.get(intMsg) == null ? 0 : closeMap.get(intMsg) + 1;
				closeMap.put(intMsg, current);
				if (current == 1 && vertex.getEdgeValue(message) == null && !vertex.getId().equals(message)) {
					outputList.add(new IntWritable(intMsg));
				}
			}
			closeMap.clear();
			vertex.setValue(outputList);
		}
		vertex.voteToHalt();
	}

	/** Quick, immutable K,V storage for sorting in tree set */
	public static class Pair implements Comparable<Pair> {

		/**
		 * key
		 * 
		 * @param key the IntWritable key
		 */
		private final Integer key;
		/**
		 * value
		 * 
		 * @param value the Integer value
		 */
		private final Integer value;

		/**
		 * Constructor
		 * 
		 * @param k the key
		 * @param v the value
		 */
		public Pair(final Integer k, final Integer v) {
			key = k;
			value = v;
		}

		/**
		 * key getter
		 * 
		 * @return the key
		 */
		public Integer getKey() {
			return key;
		}

		/**
		 * value getter
		 * 
		 * @return the value
		 */
		public Integer getValue() {
			return value;
		}

		/**
		 * Comparator to quickly sort by values
		 * 
		 * @param other the Pair to compare with THIS
		 * @return the comparison value as an integer
		 */
		@Override
		public int compareTo(final Pair other) {
			return other.value - value;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj instanceof Pair) {
				final Pair other = (Pair) obj;
				return Objects.equal(key, other.key);
			}
			return false;
		}

		@Override
		public String toString() {
			return "Pair [key=" + key + ", value=" + value + "]";
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(key);
		}
	}

	/**
	 * Utility class for delivering the array of vertices THIS vertex should connect with to close triangles with
	 * neighbors
	 */
	public static class IntArrayListWritable extends ArrayListWritable<IntWritable> {

		/** Default constructor for reflection */
		public IntArrayListWritable() {
			super();
		}

		/** Set storage type for this ArrayListWritable */
		@Override
		@SuppressWarnings("unchecked")
		public void setClass() {
			setClass(IntWritable.class);
		}
	}
}
