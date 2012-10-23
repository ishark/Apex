/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.DAG;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.GenericTestModule;
import com.malhartech.dag.Node;
import com.malhartech.dag.OperatorContext;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.TestGeneratorInputModule;
import com.malhartech.dag.TestOutputModule;
import com.malhartech.dag.TestSink;
import com.malhartech.dag.WindowGenerator;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StramLocalCluster.MockComponentFactory;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stream.BufferServerInputStream;
import com.malhartech.stream.StramTestSupport;

public class StramLocalClusterTest
{
  private static final Logger LOG = LoggerFactory.getLogger(StramLocalClusterTest.class);

  /**
   * Verify test configuration launches and stops after input terminates.
   * Test validates expected output end to end.
   * @throws Exception
   */
  @Test
  public void testLocalClusterInitShutdown() throws Exception
  {
    DAG dag = new DAG();

    TestGeneratorInputModule genNode = dag.addOperator("genNode", TestGeneratorInputModule.class);
    genNode.setMaxTuples(1);

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    node1.setEmitFormat("%s >> node1");

    File outFile = new File("./target/" + StramLocalClusterTest.class.getName() + "-testLocalClusterInitShutdown.out");
    outFile.delete();

    TestOutputModule outNode = dag.addOperator("outNode", TestOutputModule.class);
    outNode.pathSpec = outFile.toURI().toString();

    dag.addStream("fromGenNode", genNode.outport, node1.inport1);

    dag.addStream("fromNode1", node1.outport1, outNode.inport);

    dag.setMaxContainerCount(2);

    StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.setHeartbeatMonitoringEnabled(false);
    localCluster.run();

    Assert.assertTrue(outFile + " exists", outFile.exists());
    LineNumberReader lnr = new LineNumberReader(new FileReader(outFile));
    String line;
    while ((line = lnr.readLine()) != null) {
      Assert.assertTrue("line match " + line, line.matches("" + lnr.getLineNumber() + " >> node1"));
    }
    Assert.assertEquals("number lines", 2, lnr.getLineNumber());
    lnr.close();
  }

  private static class TestBufferServerSubscriber {
    final BufferServerInputStream bsi;
    final StreamContext streamContext;
    final TestSink<Object> sink;

    TestBufferServerSubscriber(PTOperator publisherOperator, String publisherPortName) {
      // sink to collect tuples emitted by the input module
      sink = new TestSink<Object>();
      String streamName = "testSinkStream";
      String sourceId = publisherOperator.id.concat(StramChild.NODE_PORT_CONCAT_SEPARATOR).concat(TestGeneratorInputModule.OUTPUT_PORT);
      streamContext = new StreamContext(streamName);
      streamContext.setSourceId(sourceId);
      streamContext.setSinkId(this.getClass().getSimpleName());
      StreamConfiguration sconf = new StreamConfiguration();
      sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, publisherOperator.container.bufferServerAddress);
      bsi = new BufferServerInputStream(new DefaultSerDe());
      bsi.setup(sconf);
      bsi.setSink("testSink", sink);
    }


    List<Object> retrieveTuples(int expectedCount, long timeoutMillis) throws InterruptedException {
      bsi.postActivate(streamContext);
      //LOG.debug("test sink activated");
      sink.waitForResultCount(1, 3000);
      Assert.assertEquals("received " + sink.collectedTuples, expectedCount, sink.collectedTuples.size());
      List<Object> result = new ArrayList<Object>(sink.collectedTuples);

      bsi.preDeactivate();
      sink.collectedTuples.clear();
      return result;
    }

  }



  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testChildRecovery() throws Exception
  {
    DAG dag = new DAG();

    TestGeneratorInputModule node1 = dag.addOperator("node1", TestGeneratorInputModule.class);
    // data will be added externally from test
    node1.setMaxTuples(0);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);

    dag.addStream("n1n2", node1.outport, node2.inport1);

    dag.validate();

    dag.getConf().setInt(DAG.STRAM_CHECKPOINT_INTERVAL_MILLIS, 0); // disable auto backup

    final ManualScheduledExecutorService wclock = new ManualScheduledExecutorService(1);

    MockComponentFactory mcf = new MockComponentFactory() {
      @Override
      public WindowGenerator setupWindowGenerator() {
        WindowGenerator wingen = StramTestSupport.setupWindowGenerator(wclock);
        return wingen;
      }
    };

    StramLocalCluster localCluster = new StramLocalCluster(dag, mcf);
    localCluster.runAsync();


    PTOperator ptNode1 = localCluster.findByLogicalNode(dag.getOperatorWrapper(node1));
    PTOperator ptNode2 = localCluster.findByLogicalNode(dag.getOperatorWrapper(node2));

    LocalStramChild c0 = waitForActivation(localCluster, ptNode1);
    Map<String, Node<?,?>> nodeMap = c0.getNodes();
    Assert.assertEquals("number operators", 1, nodeMap.size());
    TestGeneratorInputModule n1 = (TestGeneratorInputModule)nodeMap.get(ptNode1.id).getOperator();
    Assert.assertNotNull(n1);

    LocalStramChild c2 = waitForActivation(localCluster, ptNode2);
    Map<String, Node<?,?>> c2NodeMap = c2.getNodes();
    Assert.assertEquals("number operators downstream", 1, c2NodeMap.size());
    GenericTestModule n2 = (GenericTestModule)c2NodeMap.get(localCluster.findByLogicalNode(dag.getOperatorWrapper(node2)).id).getOperator();
    Assert.assertNotNull(n2);

    // sink to collect tuples emitted by the input module
    TestBufferServerSubscriber sink = new TestBufferServerSubscriber(ptNode1, TestGeneratorInputModule.OUTPUT_PORT);

    // input data
    String window0Tuple = "window0Tuple";
    n1.addTuple(window0Tuple);

    OperatorContext n1Context = c0.getNodeContext(ptNode1.id);
    Assert.assertEquals("initial window id", 0, n1Context.getLastProcessedWindowId());
    wclock.tick(1); // begin window 1
    wclock.tick(1); // begin window 2
    StramTestSupport.waitForWindowComplete(n1Context, 1);

    backupNode(c0, n1Context); // backup window 2

    wclock.tick(1); // end window 2
    StramTestSupport.waitForWindowComplete(n1Context, 2);

    OperatorContext n2Context = c2.getNodeContext(ptNode2.id);
    Assert.assertNotNull("context " + ptNode2);

    wclock.tick(1); // end window 3

    StramTestSupport.waitForWindowComplete(n2Context, 3);
    n2.setMyStringProperty("checkpoint3");
    backupNode(c2, n2Context); // backup window 4

    // move window forward, wait until propagated to module,
    // to ensure backup at previous window end was processed
    wclock.tick(1);
    StramTestSupport.waitForWindowComplete(n2Context, 4);

    // propagate checkpoints to master
    c0.triggerHeartbeat();
    // wait for heartbeat cycle to complete
    c0.waitForHeartbeat(5000);
    Assert.assertEquals("checkpoint propagated " + ptNode1, 2, ptNode1.getRecentCheckpoint());
    c2.triggerHeartbeat();
    //Thread.yield();
    Thread.sleep(1); // yield without using yield to heartbeat trigger cycle
    c2.waitForHeartbeat(5000);
    Assert.assertEquals("checkpoint propagated " + ptNode2, 4, ptNode2.getRecentCheckpoint());

    // activated test sink, verify tuple stored at buffer server
    List<Object> tuples = sink.retrieveTuples(1, 3000);
    Assert.assertEquals("received " + tuples, 1, tuples.size());
    Assert.assertEquals("received " + tuples, window0Tuple, tuples.get(0));

    // simulate node failure
    localCluster.failContainer(c0);

    // replacement container starts empty
    // operators will deploy after downstream node was removed
    LocalStramChild c0Replaced = waitForActivation(localCluster, ptNode1);
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000); // next heartbeat after setup

    Assert.assertNotSame("old container", c0, c0Replaced);
    Assert.assertNotSame("old container", c0.getContainerId(), c0Replaced.getContainerId());

    // verify change in downstream container
    LOG.debug("triggering c2 heartbeat processing");
    StramChildAgent c2Agent = localCluster.getContainerAgent(c2);

    // wait for downstream re-deploy to complete
    while (c2Agent.hasPendingWork()) {
      Thread.sleep(500);
      c2.triggerHeartbeat();
      LOG.debug("Waiting for {} to complete pending work.", c2.getContainerId());
    }

    Assert.assertEquals("downstream operators after redeploy " + c2.getNodes(), 1, c2.getNodes().size());
    // verify downstream node was replaced in same container
    Assert.assertEquals("active " + ptNode2, c2, waitForActivation(localCluster, ptNode2));
    GenericTestModule n2Replaced = (GenericTestModule)c2NodeMap.get(localCluster.findByLogicalNode(dag.getOperatorWrapper(node2)).id).getOperator();
    Assert.assertNotNull("redeployed " + ptNode2, n2Replaced);
    Assert.assertNotSame("new instance " + ptNode2, n2, n2Replaced);
    Assert.assertEquals("restored state " + ptNode2, n2.getMyStringProperty(), n2Replaced.getMyStringProperty());

    TestGeneratorInputModule n1Replaced = (TestGeneratorInputModule)nodeMap.get(ptNode1.id).getOperator();
    Assert.assertNotNull(n1Replaced);

    OperatorContext n1ReplacedContext = c0Replaced.getNodeContext(ptNode1.id);
    Assert.assertNotNull("node active " + ptNode1, n1ReplacedContext);
    // should node context should reflect last processed window (the backup window)?
    //Assert.assertEquals("initial window id", 1, n1ReplacedContext.getLastProcessedWindowId());
    wclock.tick(1);
    StramTestSupport.waitForWindowComplete(n1ReplacedContext, 5);

    // refresh n2 context after operator was re-deployed
    n2Context = c2.getNodeContext(ptNode2.id);
    Assert.assertNotNull("node active " + ptNode2, n2Context);

    StramTestSupport.waitForWindowComplete(n2Context, 5);
    backupNode(c0Replaced, n1ReplacedContext); // backup window 6
    backupNode(c2, n2Context); // backup window 6
    wclock.tick(1); // end window 6

    StramTestSupport.waitForWindowComplete(n1ReplacedContext, 6);
    StramTestSupport.waitForWindowComplete(n2Context, 6);

    // propagate checkpoints to master
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000);
    c2.triggerHeartbeat();
    c2.waitForHeartbeat(5000);

    // verify tuple sent before publisher went down remains in buffer
    // (publisher to resume from checkpoint id)
    tuples = sink.retrieveTuples(1, 3000);
    Assert.assertEquals("received " + tuples, 1, tuples.size());
    Assert.assertEquals("received " + tuples, window0Tuple, tuples.get(0));

    // purge checkpoints
    localCluster.dnmgr.monitorHeartbeat(); // checkpoint purging

    Assert.assertEquals("checkpoints " + ptNode1, Arrays.asList(new Long[] {6L}), ptNode1.checkpointWindows);
    Assert.assertEquals("checkpoints " + ptNode2, Arrays.asList(new Long[] {6L}), ptNode2.checkpointWindows);

    // buffer server data purged
    tuples = sink.retrieveTuples(0, 3000);
    Assert.assertEquals("received " + tuples, 0, tuples.size());

    localCluster.shutdown();
  }

  /**
   * Wait until instance of operator comes online in a container and return the container reference.
   *
   * @param localCluster
   * @param nodeConf
   * @return
   * @throws InterruptedException
   */
  @SuppressWarnings("SleepWhileInLoop")
  private LocalStramChild waitForActivation(StramLocalCluster localCluster, PTOperator node) throws InterruptedException
  {
    LocalStramChild container;
    while (true) {
      if (node.container.containerId != null) {
        if ((container = localCluster.getContainer(node.container.containerId)) != null) {
          if (container.getNodeContext(node.id) != null) {
            return container;
          }
        }
      }
      try {
        LOG.debug("Waiting for {} in container {}", node, node.container.containerId);
        Thread.sleep(500);
      }
      catch (InterruptedException e) {
      }
    }
  }

  private void backupNode(StramChild c, OperatorContext nodeCtx)
  {
    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(nodeCtx.getId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.nodeRequests = Collections.singletonList(backupRequest);
    LOG.debug("Requesting backup {} node {}", c.getContainerId(), nodeCtx.getId());
    c.processHeartbeatResponse(rsp);
  }

}
