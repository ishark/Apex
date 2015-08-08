package com.datatorrent.stram.stream;

import java.util.Set;

import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.stram.plan.logical.StreamCodecWrapperForPersistance;

public class PartitionAwareSinkForPersistence<T> extends PartitionAwareSink<T>
{
  StreamCodecWrapperForPersistance<Object> serdeForPersistence;

  public PartitionAwareSinkForPersistence(StreamCodec<T> serde, Set<Integer> partitions, int mask, Sink<T> output)
  {
    super(serde, partitions, mask, output);
    serdeForPersistence = (StreamCodecWrapperForPersistance<Object>) serde;
  }

  @Override
  protected boolean canPutPayloadToOutput(T payload)
  {
    if (!serdeForPersistence.shouldCaptureEvent(payload)) {
      return false;
    }

    return super.canPutPayloadToOutput(payload);
  }
}
