/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.dag.RecoverableInputOperator;
import java.util.HashSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class NodeRecoveryTest
{
  private static final Logger logger = LoggerFactory.getLogger(NodeRecoveryTest.class);
  static HashSet<Long> collection = new HashSet<Long>(20);

  public static class CollectorOperator extends BaseOperator
  {
    public final transient DefaultInputPort<Long> input = new DefaultInputPort<Long>(this)
    {
      @Override
      public void process(Long tuple)
      {
//        logger.debug("adding the tuple {}", Long.toHexString(tuple));
        collection.add(tuple);
      }
    };
  }

  @Test
  public void testInputOperatorRecovery() throws Exception
  {
    int maxTuples = 20;
    DAG dag = new DAG();
    dag.setMaxContainerCount(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();

//    logger.debug("Collected Tuples = {}", collection);
    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }
}