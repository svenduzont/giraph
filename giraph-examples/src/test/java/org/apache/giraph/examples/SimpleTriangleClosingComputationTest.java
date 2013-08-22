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

import static org.junit.Assert.assertEquals;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.SimpleTriangleClosingComputation.IntArrayListWritable;
import org.apache.giraph.graph.DefaultVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Contains a simple unit test for {@link SimpleTriangleClosingComputation}
 */
public class SimpleTriangleClosingComputationTest {

	/**
	 * Test the behavior of the triangle closing algorithm: does it send all its out edge values to all neighbors?
	 */
	@Test
	public void testSuperstepZero() throws Exception {
		// this guy should end up with an array value of 4
		final Vertex<IntWritable, IntArrayListWritable, NullWritable> vertex = new DefaultVertex<IntWritable, IntArrayListWritable, NullWritable>();

		final IntArrayListWritable alw = new IntArrayListWritable();

		final SimpleTriangleClosingComputation computation = new SimpleTriangleClosingComputation();
		final MockUtils.MockedEnvironment env = MockUtils.prepareVertexAndComputation(vertex, new IntWritable(1), alw,
				false, computation, 0L);

		vertex.addEdge(EdgeFactory.create(new IntWritable(5)));
		vertex.addEdge(EdgeFactory.create(new IntWritable(7)));

		computation.compute(vertex, Lists.<IntWritable> newArrayList(new IntWritable(83), new IntWritable(42)));

		env.verifyMessageSentToAllEdges(vertex, new IntWritable(5));
		env.verifyMessageSentToAllEdges(vertex, new IntWritable(7));
	}

	/** Test behavior of compute() with incoming messages (superstep 1) */
	@Test
	public void testSuperstepOne() throws Exception {
		// see if the vertex interprets its incoming
		// messages properly to verify the algorithm
		final Vertex<IntWritable, IntArrayListWritable, NullWritable> vertex = new DefaultVertex<IntWritable, IntArrayListWritable, NullWritable>();
		final SimpleTriangleClosingComputation computation = new SimpleTriangleClosingComputation();
		final MockUtils.MockedEnvironment env = MockUtils.prepareVertexAndComputation(vertex, new IntWritable(1), null,
				false, computation, 1L);

		// superstep 1: can the vertex process these correctly?
		computation.compute(vertex, Lists.<IntWritable> newArrayList(new IntWritable(7), new IntWritable(3),
				new IntWritable(4), new IntWritable(7), new IntWritable(4), new IntWritable(2), new IntWritable(4)));
		final String pairCheck = "[4, 7]";
		assertEquals(pairCheck, vertex.getValue().toString());
	}

	/**
	 * A local integration test on toy data
	 */
	@Test
	public void testToyData() throws Exception {

		// A small graph
		final String[] graph = new String[] { "1 4 2 3", "2 1 4 5", "3 4 1", "4 3 2 1 5", "5 2 4" };

		final GiraphConfiguration conf = new GiraphConfiguration();
		conf.setComputationClass(SimpleTriangleClosingComputation.class);
		conf.setOutEdgesClass(ByteArrayEdges.class);
		conf.setVertexInputFormatClass(IntIntArrayNullTextInputFormat.class);
		conf.setVertexOutputFormatClass(VertexWithIntArrayValueNullEdgeTextOutputFormat.class);
		// Run internally
		final Iterable<String> results = InternalVertexRunner.run(conf, graph);
		for (final String string : results) {
			System.out.println(string);
		}

	}
}
