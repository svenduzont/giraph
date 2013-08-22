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
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.SimpleTriangleClosingComputation.IntArrayListWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

/**
 * Input format for unweighted graphs with long ids and double vertex values
 */
public class IntIntArrayNullTextInputFormat extends
		TextVertexInputFormat<IntWritable, IntArrayListWritable, NullWritable> implements
		ImmutableClassesGiraphConfigurable<IntWritable, IntArrayListWritable, NullWritable> {

	/** Configuration. */
	private ImmutableClassesGiraphConfiguration<IntWritable, IntArrayListWritable, NullWritable> conf;

	@Override
	public TextVertexReader createVertexReader(final InputSplit split, final TaskAttemptContext context)
			throws IOException {
		return new IntDoubleNullDoubleVertexReader();
	}

	@Override
	public void setConf(
			final ImmutableClassesGiraphConfiguration<IntWritable, IntArrayListWritable, NullWritable> configuration) {
		conf = configuration;
	}

	@Override
	public ImmutableClassesGiraphConfiguration<IntWritable, IntArrayListWritable, NullWritable> getConf() {
		return conf;
	}

	/**
	 * Vertex reader associated with {@link IntIntArrayNullTextInputFormat}.
	 */
	public class IntDoubleNullDoubleVertexReader extends
			TextVertexInputFormat<IntWritable, IntArrayListWritable, NullWritable>.TextVertexReader {

		/** Separator of the vertex and neighbors */
		private final Pattern separator = Pattern.compile("[\t ]");

		@Override
		public Vertex<IntWritable, IntArrayListWritable, NullWritable> getCurrentVertex() throws IOException,
				InterruptedException {
			final Vertex<IntWritable, IntArrayListWritable, NullWritable> vertex = conf.createVertex();

			final String[] tokens = separator.split(getRecordReader().getCurrentValue().toString());
			final List<Edge<IntWritable, NullWritable>> edges = Lists.newArrayListWithCapacity(tokens.length - 1);
			for (int n = 1; n < tokens.length; n++) {
				edges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(tokens[n])), NullWritable.get()));
			}

			final IntWritable vertexId = new IntWritable(Integer.parseInt(tokens[0]));
			vertex.initialize(vertexId, new IntArrayListWritable(), edges);

			return vertex;
		}

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}
	}
}
