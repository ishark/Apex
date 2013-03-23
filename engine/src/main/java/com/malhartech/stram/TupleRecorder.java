/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Operator;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.engine.Tuple;
import com.malhartech.util.JacksonObjectMapperProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.core.MediaType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class TupleRecorder implements Operator
{
  public static final String INDEX_FILE = "index.txt";
  public static final String META_FILE = "meta.txt";
  public static final String VERSION = "1.0";
  private transient FileSystem fs;
  private transient FSDataOutputStream partOutStr;
  private transient FSDataOutputStream indexOutStr;
  private transient OutputStream localDataOutput;
  private transient OutputStream localIndexOutput;
  private transient String localBasePath;
  private int bytesPerPartFile = 100 * 1024;
  private long millisPerPartFile = 30 * 60 * 1000; // 30 minutes
  private String basePath = ".";
  private transient String hdfsFile;
  private int fileParts = 0;
  private int partFileTupleCount = 0;
  private int totalTupleCount = 0;
  private long currentPartFileTimeStamp = 0;
  private HashMap<String, PortInfo> portMap = new HashMap<String, PortInfo>(); // used for output portInfo <name, id> map
  private HashMap<String, PortCount> portCountMap = new HashMap<String, PortCount>(); // used for tupleCount of each port <name, count> map
  private transient long currentWindowId = -1;
  private transient ArrayList<Range> windowIdRanges = new ArrayList<Range>();
  //private transient long partBeginWindowId = -1;
  private String recordingName = "Untitled";
  private final long startTime = System.currentTimeMillis();
  private int nextPortIndex = 0;
  private HashMap<String, Sink<Object>> sinks = new HashMap<String, Sink<Object>>();
  private transient long endWindowTuplesProcessed = 0;
  private boolean isLocalMode = false;
  private Class<? extends StreamCodec> streamCodecClass = JsonStreamCodec.class;
  private transient StreamCodec<Object> streamCodec;
  private transient JsonStreamCodec<Object> jsonStreamCodec = new JsonStreamCodec<Object>();
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TupleRecorder.class);
  private boolean syncRequested = false;
  private Client client;
  private URI postToUrl = null;
  private URI getNumSubscribersUrl = null;
  private int numSubscribers = 0;

  public TupleRecorder()
  {
    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(JacksonObjectMapperProvider.class);
    client = Client.create(cc);
  }

  public RecorderSink newSink(String key)
  {
    RecorderSink recorderSink = new RecorderSink(key);
    sinks.put(key, recorderSink);
    return recorderSink;
  }

  /**
   * Sets the stream codec for serialization to write to the files
   * The serialization method must not produce newlines.
   * For serializations that produces binary, base64 is recommended.
   *
   * @param streamCodecClass
   */
  public void setStreamCodec(Class<? extends StreamCodec<Object>> streamCodecClass)
  {
    this.streamCodecClass = streamCodecClass;
  }

  public void setPostToUrl(String postToUrl) throws URISyntaxException
  {
    this.postToUrl = new URI(postToUrl);
  }

  public void setGetNumSubscribersUrl(String getNumSubscribersUrl) throws URISyntaxException
  {
    this.getNumSubscribersUrl = new URI(getNumSubscribersUrl);
  }

  public HashMap<String, PortInfo> getPortInfoMap()
  {
    return portMap;
  }

  public int getTotalTupleCount()
  {
    return totalTupleCount;
  }

  public HashMap<String, Sink<Object>> getSinkMap()
  {
    return sinks;
  }

  public void setLocalMode(boolean isLocalMode)
  {
    this.isLocalMode = isLocalMode;
  }

  /* defined for json information */
  public static class PortInfo
  {
    public String name;
    public String streamName;
    public String type;
    public int id;
  }

  /* defined for written tuple count of each port recorded in index file */
  public static class PortCount
  {
    public int id;
    public long count;
  }

  public static class RecordInfo
  {
    public long startTime;
    public String recordingName;
  }

  public static class Range
  {
    public long low = -1;
    public long high = -1;

    public Range()
    {
    }

    public Range(long low, long high)
    {
      this.low = low;
      this.high = high;
    }

    @Override
    public String toString()
    {
      return "[" + String.valueOf(low) + "," + String.valueOf(high) + "]";
    }

  }

  public String getRecordingName()
  {
    return recordingName;
  }

  public void setRecordingName(String recordingName)
  {
    this.recordingName = recordingName;
  }

  public void setBytesPerPartFile(int bytes)
  {
    this.bytesPerPartFile = bytes;
  }

  public void setMillisPerPartFile(long millis)
  {
    this.millisPerPartFile = millis;
  }

  public void setBasePath(String path)
  {
    this.basePath = path;
  }

  public String getBasePath()
  {
    return basePath;
  }

  public long getStartTime()
  {
    return startTime;
  }

  public void addInputPortInfo(String portName, String streamName)
  {
    PortInfo portInfo = new PortInfo();
    portInfo.name = portName;
    portInfo.streamName = streamName;
    portInfo.type = "input";
    portInfo.id = nextPortIndex++;
    portMap.put(portName, portInfo);
    PortCount pc = new PortCount();
    pc.id = portInfo.id;
    pc.count = 0;
    portCountMap.put(portName, pc);
  }

  public void addOutputPortInfo(String portName, String streamName)
  {
    PortInfo portInfo = new PortInfo();
    portInfo.name = portName;
    portInfo.streamName = streamName;
    portInfo.type = "output";
    portInfo.id = nextPortIndex++;
    portMap.put(portName, portInfo);
    PortCount pc = new PortCount();
    pc.id = portInfo.id;
    pc.count = 0;
    portCountMap.put(portName, pc);
  }

  @Override
  public void teardown()
  {
    logger.info("Closing down tuple recorder.");
    try {
      if (partOutStr != null) {
        logger.debug("Closing part file");
        partOutStr.close();
        if (indexOutStr != null) {
          writeIndex();
          writeIndexEnd();
        }
      }
      if (indexOutStr != null) {
        indexOutStr.close();
      }
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setup(OperatorContext context)
  {
    try {
      streamCodec = streamCodecClass.newInstance();
      Path pa = new Path(basePath, META_FILE);
      if (basePath.startsWith("file:")) {
        isLocalMode = true;
        localBasePath = basePath.substring(5);
        (new File(localBasePath)).mkdirs();
      }
      fs = FileSystem.get(pa.toUri(), new Configuration());
      FSDataOutputStream metaOs = fs.create(pa);

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write((VERSION + "\n").getBytes());

      RecordInfo recordInfo = new RecordInfo();
      recordInfo.startTime = startTime;
      recordInfo.recordingName = recordingName;
      bos.write(streamCodec.toByteArray(recordInfo).data);
      bos.write("\n".getBytes());

      for (PortInfo pi: portMap.values()) {
        bos.write(streamCodec.toByteArray(pi).data);
        bos.write("\n".getBytes());
      }

      metaOs.write(bos.toByteArray());
      metaOs.hflush();
      metaOs.close();

      pa = new Path(basePath, INDEX_FILE);
      if (isLocalMode) {
        localIndexOutput = new FileOutputStream(localBasePath + "/" + INDEX_FILE);
        indexOutStr = new FSDataOutputStream(localIndexOutput, null);
      }
      else {
        indexOutStr = fs.create(pa);
      }
    }
    catch (Exception ex) {
      logger.error("Trouble setting up tuple recorder", ex);
    }
  }

  protected void openNewPartFile() throws IOException
  {
    hdfsFile = "part" + fileParts + ".txt";
    Path path = new Path(basePath, hdfsFile);
    logger.debug("Opening new part file: {}", hdfsFile);
    if (isLocalMode) {
      localDataOutput = new FileOutputStream(localBasePath + "/" + hdfsFile);
      partOutStr = new FSDataOutputStream(localDataOutput, null);
    }
    else {
      partOutStr = fs.create(path);
    }
    fileParts++;
    currentPartFileTimeStamp = System.currentTimeMillis();
    partFileTupleCount = 0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (this.currentWindowId != windowId) {
      if (windowId != this.currentWindowId + 1) {
        if (!windowIdRanges.isEmpty()) {
          windowIdRanges.get(windowIdRanges.size() - 1).high = this.currentWindowId;
        }
        Range range = new Range();
        range.low = windowId;
        windowIdRanges.add(range);
      }
      if (windowIdRanges.isEmpty()) {
        Range range = new Range();
        range.low = windowId;
        windowIdRanges.add(range);
      }
      this.currentWindowId = windowId;
      endWindowTuplesProcessed = 0;
      try {
        if (partOutStr == null) {
          openNewPartFile();
        }
        logger.debug("Writing begin window (id: {}) to tuple recorder", windowId);
        partOutStr.write(("B:" + windowId + "\n").getBytes());
        //fsOutput.hflush();
      }
      catch (IOException ex) {
        logger.error(ex.toString());
      }
    }
  }

  @Override
  public void endWindow()
  {
    if (++endWindowTuplesProcessed == portMap.size()) {
      try {
        partOutStr.write(("E:" + currentWindowId + "\n").getBytes());
        logger.debug("Got last end window tuple.  Flushing...");
        partOutStr.hflush();
        if (syncRequested || (partOutStr.getPos() > bytesPerPartFile) || (currentPartFileTimeStamp + millisPerPartFile < System.currentTimeMillis())) {
          partOutStr.close();
          partOutStr = null;
          writeIndex();
          syncRequested = false;
          logger.debug("Closing current part file.");
        }
        updateNumSubscribers();
      }
      catch (IOException ex) {
        logger.error(ex.toString());
      }
    }
  }

  public void requestSync()
  {
    syncRequested = true;
  }

  public void writeTuple(Object obj, String port)
  {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write(streamCodec.toByteArray(obj).data);
      bos.write("\n".getBytes());

      PortInfo pi = portMap.get(port);
      String str = "T:" + pi.id + ":" + bos.size() + ":";
      PortCount pc = portCountMap.get(port);
      pc.count++;
      portCountMap.put(port, pc);

      partOutStr.write(str.getBytes());
      partOutStr.write(bos.toByteArray());
      //logger.debug("Writing tuple for port id {}", pi.id);
      //fsOutput.hflush();
      if (numSubscribers > 0) {
        postTupleData(pi.id, obj);
      }
      ++partFileTupleCount;
      ++totalTupleCount;
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  public void writeControlTuple(Tuple tuple, String port)
  {
    try {
      PortInfo pi = portMap.get(port);
      String str = "C:" + pi.id; // to be completed when Tuple is externalizable
      if (partOutStr == null) {
        openNewPartFile();
      }
      partOutStr.write(str.getBytes());
      partOutStr.write("\n".getBytes());
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  private static String convertToString(List<Range> ranges)
  {
    String result = "";
    int i = 0;
    for (Range range: ranges) {
      if (i++ > 0) {
        result += ",";
      }
      result += String.valueOf(range.low);
      result += "-";
      result += String.valueOf(range.high);
    }
    return result;
  }

  public void writeIndex()
  {
    if (windowIdRanges.isEmpty()) {
      return;
    }
    windowIdRanges.get(windowIdRanges.size() - 1).high = this.currentWindowId;
    logger.debug("Writing index file for windows {}", windowIdRanges);
    try {
      indexOutStr.write(("F:" + convertToString(windowIdRanges) + ":T:" + partFileTupleCount + ":").getBytes());

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      int i = 0;
      String countStr = "{";
      for (String key: portCountMap.keySet()) {
        PortCount pc = portCountMap.get(key);
        if (i != 0) {
          countStr += ",";
        }
        countStr += "\"" + pc.id + "\"" + ":\"" + pc.count + "\"";
        i++;

        pc.count = 0;
        portCountMap.put(key, pc);
      }
      countStr += "}";
      bos.write(countStr.getBytes());
      partFileTupleCount = 0;

      indexOutStr.write((String.valueOf(bos.size()) + ":").getBytes());
      indexOutStr.write(bos.toByteArray());
      indexOutStr.write((":" + hdfsFile + "\n").getBytes());
      indexOutStr.hflush();
      indexOutStr.hsync();
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
    windowIdRanges.clear();
  }

  public void writeIndexEnd()
  {
    try {
      indexOutStr.write(("E\n").getBytes());
      indexOutStr.hflush();
      indexOutStr.hsync();
    }
    catch (IOException ex) {
      logger.error(ex.toString());
    }
  }

  private void postTupleData(int portId, Object obj)
  {
    try {
      if (postToUrl != null) {
        JSONObject json = new JSONObject();
        json.put("portId", String.valueOf(portId));
        json.put("windowId", currentWindowId);
        json.put("data", new JSONObject(new String(jsonStreamCodec.toByteArray(obj).data)));
        WebResource wr = client.resource(postToUrl);
        wr.type(MediaType.APPLICATION_JSON).post(json);
      }
    }
    catch (Exception ex) {
      logger.warn("Error posting to URL {}", postToUrl, ex);
    }
  }

  public void updateNumSubscribers()
  {
    try {
      if (getNumSubscribersUrl != null) {
        WebResource wr = client.resource(getNumSubscribersUrl);
        JSONObject response = wr.get(JSONObject.class);
        numSubscribers = response.getInt("num");
        logger.info("Number of subscribers is {}", numSubscribers);
      }
    }
    catch (Exception ex) {
      numSubscribers = 0;
      logger.warn("Error getting number of subscribers from URL {}", getNumSubscribersUrl, ex);
    }
  }

  public class RecorderSink implements Sink<Object>
  {
    private final String portName;

    public RecorderSink(String portName)
    {
      this.portName = portName;
    }

    @Override
    public void process(Object payload)
    {
      // *** if it's not a control tuple, then (payload instanceof Tuple) returns false
      // In other words, if it's a regular tuple emitted by operators (payload), payload
      // is not an instance of Tuple (confusing... I know)
      if (payload instanceof Tuple) {
        Tuple tuple = (Tuple)payload;
        MessageType messageType = tuple.getType();
        if (messageType == MessageType.BEGIN_WINDOW) {
          beginWindow(tuple.getWindowId());
        }
        writeControlTuple(tuple, portName);
        if (messageType == MessageType.END_WINDOW) {
          endWindow();
        }
      }
      else {
        writeTuple(payload, portName);
      }
    }

  }

}