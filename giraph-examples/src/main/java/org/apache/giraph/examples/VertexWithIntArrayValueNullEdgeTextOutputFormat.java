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

import org.apache.giraph.examples.SimpleTriangleClosingComputation.IntArrayListWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Output format for vertices with a long as id, a double as value and null edges
 */
public class VertexWithIntArrayValueNullEdgeTextOutputFormat extends
		TextVertexOutputFormat<IntWritable, IntArrayListWritable, NullWritable> {

	@Override
	public TextVertexWriter createVertexWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
		return new VertexWithIntArrayValueWriter();
	}

	/**
	 * Vertex writer used with {@link VertexWithIntArrayValueNullEdgeTextOutputFormat}.
	 */
	public class VertexWithIntArrayValueWriter extends TextVertexWriter {

		@Override
		public void writeVertex(final Vertex<IntWritable, IntArrayListWritable, NullWritable> vertex) throws IOException,
				InterruptedException {
			final StringBuilder output = new StringBuilder();
			output.append(vertex.getId().get());
			output.append('\t');
			output.append(vertex.getValue());
			getRecordWriter().write(new Text(output.toString()), null);
		}
	}
}
