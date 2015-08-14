package com.datatorrent.stram.plan.logical;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;

/**
 * This codec is used for serializing the objects of class which are Kryo
 * serializable. Used for stream codec wrapper used for persistence
 */
public class DefaultKryoStreamCodec<T> implements StreamCodec<T>, Serializable
{
  protected transient Kryo kryo;

  public DefaultKryoStreamCodec()
  {
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  @Override
  public Object fromByteArray(Slice fragment)
  {
    ByteArrayInputStream is = new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
    Input input = new Input(is);
    return kryo.readClassAndObject(input);
  }

  @Override
  public Slice toByteArray(T info)
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);

    kryo.writeClassAndObject(output, info);
    output.flush();
    return new Slice(os.toByteArray(), 0, os.toByteArray().length);
  }

  @Override
  public int getPartition(T t)
  {
    return t.hashCode();
  }
}
