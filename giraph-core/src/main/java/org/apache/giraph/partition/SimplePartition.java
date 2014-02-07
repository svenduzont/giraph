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

package org.apache.giraph.partition;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;
import org.python.google.common.io.Files;

import com.google.common.collect.Maps;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.giraph.conf.GiraphConstants.USE_OUT_OF_CORE_MESSAGES;

/**
 * A simple map-based container that stores vertices. Vertex ids will map to exactly one partition.
 * 
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class SimplePartition<I extends WritableComparable, V extends Writable, E extends Writable> extends
		BasicPartition<I, V, E> {

	/** Vertex map for this range (keyed by index) */
	private ConcurrentMap<I, Vertex<I, V, E>> vertexMap;
	private Object nullId;

	/**
	 * Constructor for reflection.
	 */
	public SimplePartition() {}

	public void dump(File f) {
		final Set<Entry<I, Vertex<I, V, E>>> entrySet = vertexMap.entrySet();
		BufferedWriter writer = null;
		try {
			writer = Files.newWriter(f, Charset.defaultCharset());
			for (Entry<I, Vertex<I, V, E>> entry : entrySet) {
				writer.append(entry.toString()).append("\n");
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
		}
		erzr();
	}

	protected void erzr() {
		if (nullId == null) {
			return;
		}

		final Set<I> keySet = vertexMap.keySet();
		for (I i : keySet) {
			System.out.println(i);
		}
		final Vertex<I, V, E> vertex = vertexMap.get(nullId);
		System.out.println(vertex);
	}

	@Override
	public void initialize(int partitionId, Progressable progressable) {
		super.initialize(partitionId, progressable);
		if (USE_OUT_OF_CORE_MESSAGES.get(getConf())) {
			vertexMap = new ConcurrentSkipListMap<I, Vertex<I, V, E>>();
		} else {
			vertexMap = Maps.newConcurrentMap();
		}
	}

	@Override
	public Vertex<I, V, E> getVertex(I vertexIndex) {
		final Vertex<I, V, E> vertex = vertexMap.get(vertexIndex);
		if (vertex == null) {
			this.nullId = vertexIndex;
		}
		return vertex;
	}

	@Override
	public Vertex<I, V, E> putVertex(Vertex<I, V, E> vertex) {
		return vertexMap.put(vertex.getId(), vertex);
	}

	@Override
	public Vertex<I, V, E> removeVertex(I vertexIndex) {
		return vertexMap.remove(vertexIndex);
	}

	@Override
	public void addPartition(Partition<I, V, E> partition) {
		for (Vertex<I, V, E> vertex : partition) {
			vertexMap.put(vertex.getId(), vertex);
		}
	}

	@Override
	public long getVertexCount() {
		return vertexMap.size();
	}

	@Override
	public long getEdgeCount() {
		long edges = 0;
		for (Vertex<I, V, E> vertex : vertexMap.values()) {
			edges += vertex.getNumEdges();
		}
		return edges;
	}

	@Override
	public void saveVertex(Vertex<I, V, E> vertex) {
		// No-op, vertices are stored as Java objects in this partition
	}

	@Override
	public String toString() {
		return "(id=" + getId() + ",V=" + vertexMap.size() + ")";
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		if (USE_OUT_OF_CORE_MESSAGES.get(getConf())) {
			vertexMap = new ConcurrentSkipListMap<I, Vertex<I, V, E>>();
		} else {
			vertexMap = Maps.newConcurrentMap();
		}
		int vertices = input.readInt();
		for (int i = 0; i < vertices; ++i) {
			progress();
			Vertex<I, V, E> vertex = WritableUtils.readVertexFromDataInput(input, getConf());
			if (vertexMap.put(vertex.getId(), vertex) != null) {
				throw new IllegalStateException("readFields: " + this + " already has same id " + vertex);
			}
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		super.write(output);
		output.writeInt(vertexMap.size());
		for (Vertex<I, V, E> vertex : vertexMap.values()) {
			progress();
			WritableUtils.writeVertexToDataOutput(output, vertex, getConf());
		}
	}

	@Override
	public Iterator<Vertex<I, V, E>> iterator() {
		return vertexMap.values().iterator();
	}
}
