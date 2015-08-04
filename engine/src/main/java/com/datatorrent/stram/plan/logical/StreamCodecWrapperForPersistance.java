package com.datatorrent.stram.plan.logical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.codec.JsonStreamCodec;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;

public class StreamCodecWrapperForPersistance<T> implements StreamCodec<T>, Serializable {

  private StreamCodec<Object> specifiedStreamCodec;
  public Map<InputPortMeta, Collection<PartitionKeys>> inputPortToPartitionMap;
  public Map<InputPortMeta, StreamCodec<Object>> codecsToMerge;
  public ArrayList<PartitionKeys> persistOperatorPartitionKeys;
  private Map<PartitionKeys, Integer> invalidPartionNumbersPerPartition;
  private boolean operatorPartitioned;

  public StreamCodecWrapperForPersistance(Map<InputPortMeta, StreamCodec<Object>> inputStreamCodecs, StreamCodec<Object> specifiedStreamCodec) {
    this.codecsToMerge = inputStreamCodecs;
    this.setSpecifiedStreamCodec(specifiedStreamCodec);
    inputPortToPartitionMap = new HashMap<InputPortMeta, Collection<PartitionKeys>>();
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
    for (Entry<InputPortMeta, Collection<PartitionKeys>> entry : inputPortToPartitionMap.entrySet()) {
      StreamCodec<Object> codec = codecsToMerge.get(entry.getKey());
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
    partition =  partition >  persistOperatorPartitionKeys.size() ? persistOperatorPartitionKeys.size(): partition;
    return invalidPartionNumbersPerPartition.get(persistOperatorPartitionKeys.get(partition - 1));
  }

  public StreamCodec<Object> getSpecifiedStreamCodec() {
    if (specifiedStreamCodec == null) {
      // TODO: Check default stream codec to use
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
