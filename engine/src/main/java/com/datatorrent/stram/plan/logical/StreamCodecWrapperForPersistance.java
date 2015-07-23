package com.datatorrent.stram.plan.logical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.codec.JsonStreamCodec;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;

public class StreamCodecWrapperForPersistance<T> implements StreamCodec<T>, Serializable {

  private Set<StreamCodec<Object>> codecsToMerge;
  private StreamCodec<Object> specifiedStreamCodec;
  public Map<InputPortMeta, Collection<PartitionKeys>> codecsToMergeWithPartitions;
  public ArrayList<PartitionKeys> persistOperatorPartitionKeys;
  private Map<PartitionKeys, Integer> invalidPartionNumbersPerPartition;
  private boolean operatorPartitioned;

  public StreamCodecWrapperForPersistance(Set<StreamCodec<Object>> inputStreamCodecs, StreamCodec<Object> specifiedStreamCodec) {
    this.codecsToMerge = inputStreamCodecs;
    this.setSpecifiedStreamCodec(specifiedStreamCodec);
    codecsToMergeWithPartitions = new HashMap<InputPortMeta, Collection<PartitionKeys>>();
    invalidPartionNumbersPerPartition = new HashMap<PartitionKeys, Integer>();
  }

  @Override
  public Object fromByteArray(Slice fragment) {
    return getSpecifiedStreamCodec().fromByteArray(fragment);
  }

  @Override
  public Slice toByteArray(T o) {
    return getSpecifiedStreamCodec().toByteArray(o);
  }

  @Override
  public int getPartition(T o) {
    int partition = isOperatorPartitioned() ? getSpecifiedStreamCodec().getPartition(o) : persistOperatorPartitionKeys.get(0).partitions.iterator().next();
    for (Entry<InputPortMeta, Collection<PartitionKeys>> entry : codecsToMergeWithPartitions.entrySet()) {
      InputPort<?> sinkPort = entry.getKey().getPortObject();
      StreamCodec<Object> codec = (StreamCodec<Object>) sinkPort.getStreamCodec();
      Collection<PartitionKeys> partitionKeysList = entry.getValue();

      for (PartitionKeys keys : partitionKeysList) {
        if (keys.partitions.contains(keys.mask & codec.getPartition(o))) {
          // Then at least one of the partitions is getting this event
          // So send the event to persist operator
          return partition;
        }
      }
    }

    // Return some invalid partition value, so that the event wont be sent to
    // any partition

    return invalidPartionNumbersPerPartition.get(persistOperatorPartitionKeys.get(partition - 1));
  }

  public Set<StreamCodec<Object>> getCodecsToMerge() {
    return codecsToMerge;
  }

  public void setCodecsToMerge(Set<StreamCodec<Object>> codecsToMerge) {
    this.codecsToMerge = codecsToMerge;
  }

  public StreamCodec<Object> getSpecifiedStreamCodec() {
    if (specifiedStreamCodec == null) {
      //TODO: Check default stream codec to use
      specifiedStreamCodec = new JsonStreamCodec<Object>();
    }
    return specifiedStreamCodec;
  }

  public void setSpecifiedStreamCodec(StreamCodec<Object> specifiedStreamCodec) {
    this.specifiedStreamCodec = specifiedStreamCodec;
  }

  public boolean isOperatorPartitioned() {
    return operatorPartitioned;
  }

  public void setOperatorPartitioned(boolean operatorPartitioned) {
    this.operatorPartitioned = operatorPartitioned;
  }

  public void setInvalidPartionNumber(PartitionKeys partition, int invalidPartionNumber) {
    invalidPartionNumbersPerPartition.put(partition, invalidPartionNumber);
  }

}
