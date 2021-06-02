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
    name = "Breadth first search",
    description = "Finds the path length to every vertex from a selected vertex"
)
public class BreadthFirstSearchComputation extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("BreadthFirstSearch.sourceId", 1,
          "The source ID");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(BreadthFirstSearchComputation.class);

  /**
   * Is this vertex the source id?
   *
   * @param vertex Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(isSource(vertex) ? 0 : Double.MAX_VALUE));
      if (isSource(vertex))
      {
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
          sendMessage(edge.getTargetVertexId(), new DoubleWritable(1d));
        }
      }
      vertex.voteToHalt();
      return;
    }
    if (vertex.getValue().get() == Double.MAX_VALUE)
    {
      double val = -1d;
      for (DoubleWritable message : messages) {
        val = message.get();
        break;
      }
      DoubleWritable bfsValue = new DoubleWritable(val);
      DoubleWritable nextBfsValue = new DoubleWritable(val+1);
      vertex.setValue(bfsValue);
      LOG.info("Vertex " + vertex.getId() + " got bfsvalue = " + val);
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {        
        sendMessage(edge.getTargetVertexId(), nextBfsValue);
      }      
    }
    vertex.voteToHalt();
  }
}
