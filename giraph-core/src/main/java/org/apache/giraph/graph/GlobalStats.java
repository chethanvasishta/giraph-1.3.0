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

package org.apache.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.giraph.bsp.checkpoints.CheckpointStatus;
import org.apache.giraph.partition.PartitionStats;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * Aggregated stats by the master.
 */
public class GlobalStats implements Writable {
	/** All vertices in the application */
	private long vertexCount = 0;
	/** All finished vertices in the last superstep */
	private long finishedVertexCount = 0;
	/** All edges in the last superstep */
	private long edgeCount = 0;
	/** All messages sent in the last superstep */
	private long messageCount = 0;
	/** All message bytes sent in the last superstep */
	private long messageBytesCount = 0;
	/** Whether the computation should be halted */
	private boolean haltComputation = false;
	/** Bytes of data stored to disk in the last superstep */
	private long oocStoreBytesCount = 0;
	/** Bytes of data loaded to disk in the last superstep */
	private long oocLoadBytesCount = 0;
	/** Lowest percentage of graph in memory throughout the execution */
	private int lowestGraphPercentageInMemory = 100;
	/** Graphite Interval Vertices Count */
	private RangeMap<Integer, Integer> numOfIntervalVertices;
	/**
	 * Master's decision on whether we should checkpoint and what to do next.
	 */
	private CheckpointStatus checkpointStatus = CheckpointStatus.NONE;

	/**
	 * Add the stats of a partition to the global stats.
	 *
	 * @param partitionStats Partition stats to be added.
	 */
	public void addPartitionStats(PartitionStats partitionStats) {
		this.vertexCount += partitionStats.getVertexCount();
		this.finishedVertexCount += partitionStats.getFinishedVertexCount();
		this.edgeCount += partitionStats.getEdgeCount();

		if (partitionStats.getIntervalVertexCount() != null) {
			if (numOfIntervalVertices == null) {
				numOfIntervalVertices = TreeRangeMap.<Integer, Integer>create();
			}
			this.numOfIntervalVertices = merge(numOfIntervalVertices, partitionStats.getIntervalVertexCount());
		}
	}

	/**GRAPHITE */
	public RangeMap<Integer, Integer> merge(RangeMap<Integer, Integer> currentIntervalVertexCount,
			RangeMap<Integer, Integer> partitionCount) {

		TreeMap<Integer, Integer> overlaps = new TreeMap<Integer, Integer>();
		Integer timePoint;

		RangeMap<Integer, Integer> mergedIntervalVertexCount = TreeRangeMap.<Integer, Integer>create();

		for (Entry<Range<Integer>, Integer> entry : currentIntervalVertexCount.asMapOfRanges().entrySet()) {
			timePoint = entry.getKey().lowerEndpoint();
			overlaps.put(timePoint, overlaps.containsKey(timePoint) ? overlaps.get(timePoint) + 1 : 1);

			timePoint = entry.getKey().upperEndpoint();
			overlaps.put(timePoint, overlaps.containsKey(timePoint) ? overlaps.get(timePoint) - 1 : -1);
		}

		for (Entry<Range<Integer>, Integer> entry : partitionCount.asMapOfRanges().entrySet()) {
			timePoint = entry.getKey().lowerEndpoint();
			overlaps.put(timePoint, overlaps.containsKey(timePoint) ? overlaps.get(timePoint) + 1 : 1);

			timePoint = entry.getKey().upperEndpoint();
			overlaps.put(timePoint, overlaps.containsKey(timePoint) ? overlaps.get(timePoint) - 1 : -1);
		}

		RangeMap<Integer, Integer> elementaryIntervals = TreeRangeMap.<Integer, Integer>create();
		int overlap = 0;
		boolean flag = false;
		Integer last = 0;

		for (Integer point : overlaps.keySet()) {
			if (flag)
				elementaryIntervals.put(Range.closedOpen(last, point), -1);

			overlap += overlaps.get(point);
			last = point;
			flag = overlap > 0;
		}

		Integer subIntervalCount;
		for (Entry<Range<Integer>, Integer> entry : currentIntervalVertexCount.asMapOfRanges().entrySet()) {
			RangeMap<Integer, Integer> intersection = elementaryIntervals.subRangeMap(entry.getKey());
			for (Entry<Range<Integer>, Integer> subIntervalEntry : intersection.asMapOfRanges().entrySet()) {
				subIntervalCount = mergedIntervalVertexCount.get(subIntervalEntry.getKey().lowerEndpoint());
				if (subIntervalCount != null) {
					mergedIntervalVertexCount.put(subIntervalEntry.getKey(), subIntervalCount + entry.getValue());
				} else {
					mergedIntervalVertexCount.put(subIntervalEntry.getKey(), entry.getValue());
				}
			}
		}

		for (Entry<Range<Integer>, Integer> entry : partitionCount.asMapOfRanges().entrySet()) {
			RangeMap<Integer, Integer> intersection = elementaryIntervals.subRangeMap(entry.getKey());
			for (Entry<Range<Integer>, Integer> subIntervalEntry : intersection.asMapOfRanges().entrySet()) {
				subIntervalCount = mergedIntervalVertexCount.get(subIntervalEntry.getKey().lowerEndpoint());
				if (subIntervalCount != null) {
					mergedIntervalVertexCount.put(subIntervalEntry.getKey(), subIntervalCount + entry.getValue());
				} else {
					mergedIntervalVertexCount.put(subIntervalEntry.getKey(), entry.getValue());
				}
			}
		}
		return mergedIntervalVertexCount;
	}

	public long getVertexCount() {
		return vertexCount;
	}

	public long getFinishedVertexCount() {
		return finishedVertexCount;
	}

	public RangeMap<Integer, Integer> getIntervalVertexCount() {
		return this.numOfIntervalVertices;
	}
	
	public void setIntervalVertexCount(RangeMap<Integer, Integer> numOfIntervalVertices) {
		this.numOfIntervalVertices = numOfIntervalVertices;
	}
	
	public long getEdgeCount() {
		return edgeCount;
	}

	public long getMessageCount() {
		return messageCount;
	}

	public long getMessageBytesCount() {
		return messageBytesCount;
	}

	public boolean getHaltComputation() {
		return haltComputation;
	}

	public void setHaltComputation(boolean value) {
		haltComputation = value;
	}

	public long getOocStoreBytesCount() {
		return oocStoreBytesCount;
	}

	public long getOocLoadBytesCount() {
		return oocLoadBytesCount;
	}

	public CheckpointStatus getCheckpointStatus() {
		return checkpointStatus;
	}

	public void setCheckpointStatus(CheckpointStatus checkpointStatus) {
		this.checkpointStatus = checkpointStatus;
	}

	public int getLowestGraphPercentageInMemory() {
		return lowestGraphPercentageInMemory;
	}

	public void setLowestGraphPercentageInMemory(int lowestGraphPercentageInMemory) {
		this.lowestGraphPercentageInMemory = lowestGraphPercentageInMemory;
	}

	/**
	 * Add bytes loaded to the global stats.
	 *
	 * @param oocLoadBytesCount number of bytes to be added
	 */
	public void addOocLoadBytesCount(long oocLoadBytesCount) {
		this.oocLoadBytesCount += oocLoadBytesCount;
	}

	/**
	 * Add bytes stored to the global stats.
	 *
	 * @param oocStoreBytesCount number of bytes to be added
	 */
	public void addOocStoreBytesCount(long oocStoreBytesCount) {
		this.oocStoreBytesCount += oocStoreBytesCount;
	}

	/**
	 * Add messages to the global stats.
	 *
	 * @param messageCount Number of messages to be added.
	 */
	public void addMessageCount(long messageCount) {
		this.messageCount += messageCount;
	}

	/**
	 * Add messages to the global stats.
	 *
	 * @param msgBytesCount Number of message bytes to be added.
	 */
	public void addMessageBytesCount(long msgBytesCount) {
		this.messageBytesCount += msgBytesCount;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		vertexCount = input.readLong();
		finishedVertexCount = input.readLong();
		edgeCount = input.readLong();
		messageCount = input.readLong();
		messageBytesCount = input.readLong();
		oocLoadBytesCount = input.readLong();
		oocStoreBytesCount = input.readLong();
		lowestGraphPercentageInMemory = input.readInt();
		haltComputation = input.readBoolean();
		if (input.readBoolean()) {
			checkpointStatus = CheckpointStatus.values()[input.readInt()];
		} else {
			checkpointStatus = null;
		}
		
		int intervalCountSize = input.readInt();
		numOfIntervalVertices = TreeRangeMap.<Integer, Integer>create();
		if(intervalCountSize>0) {
			int start, end, count;
			for(int i=0;i<intervalCountSize;i++) {
				start = input.readInt();
				end = input.readInt();
				count = input.readInt();
				numOfIntervalVertices.put(Range.closedOpen(start, end), count);
			}
		}
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(vertexCount);
		output.writeLong(finishedVertexCount);
		output.writeLong(edgeCount);
		output.writeLong(messageCount);
		output.writeLong(messageBytesCount);
		output.writeLong(oocLoadBytesCount);
		output.writeLong(oocStoreBytesCount);
		output.writeInt(lowestGraphPercentageInMemory);
		output.writeBoolean(haltComputation);
		output.writeBoolean(checkpointStatus != null);
		if (checkpointStatus != null) {
			output.writeInt(checkpointStatus.ordinal());
		}
		
		if(numOfIntervalVertices!=null && numOfIntervalVertices.asMapOfRanges().size()>0) {
			output.writeInt(numOfIntervalVertices.asMapOfRanges().size());
			for(Entry<Range<Integer>, Integer> subIntervalCount : numOfIntervalVertices.asMapOfRanges().entrySet()) {
				output.writeInt(subIntervalCount.getKey().lowerEndpoint());
				output.writeInt(subIntervalCount.getKey().upperEndpoint());
				output.writeInt(subIntervalCount.getValue());
			}
		} else {
			output.writeInt(0);
		}
	}

	@Override
	public String toString() {
		return "(vtx=" + vertexCount + ",finVtx=" + finishedVertexCount + ",edges=" + edgeCount + ",msgCount="
				+ messageCount + ",msgBytesCount=" + messageBytesCount + ",haltComputation=" + haltComputation
				+ ", checkpointStatus=" + checkpointStatus + ')';
	}
}
