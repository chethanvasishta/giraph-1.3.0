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

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import com.google.common.collect.Iterables;

import java.io.IOException;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Compute k-Core",
    description = "Finds the graph where all vertices have at least k neigbours length"
)
public class KCoreComputation extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  public static final LongConfOption K_VALUE =
    new LongConfOption("KCoreComputation.k", 3,
          "k value for k-Core");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(KCoreComputation.class);

  private boolean hasKOrMoreNeighbours(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getNumEdges() >= K_VALUE.get(getConf());
  }

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {

    if (getSuperstep() % 2 == 0)
    {
      if (hasKOrMoreNeighbours(vertex)) {
        vertex.setValue(new DoubleWritable(vertex.getNumEdges()));
        vertex.voteToHalt();        
      } else {
        LOG.info("Removing vertex " + vertex.getId());

        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
          sendMessage(edge.getTargetVertexId(), new DoubleWritable(vertex.getId().get()));
          removeEdgesRequest(vertex.getId(), edge.getTargetVertexId());
        }
        removeVertexRequest(vertex.getId());
        vertex.voteToHalt();
      }
    }
    else {
      // remove the edges that were asked to be removed
      for (DoubleWritable message : messages) {
        long vertexID = (long)message.get();
        removeEdgesRequest(vertex.getId(), new LongWritable(vertexID));
      }
    }
  }
}
