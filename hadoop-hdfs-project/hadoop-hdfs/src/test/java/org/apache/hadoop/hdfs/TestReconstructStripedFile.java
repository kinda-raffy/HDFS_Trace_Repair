/**
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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
// [TODO] Narrow scope.
import java.util.*;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.BitSet;
// import java.util.Collections;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Random;
import java.util.stream.Collectors;

import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.erasurecode.ErasureCodingTestHelper;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

public class TestReconstructStripedFile {
  public static final Logger LOG =
          LoggerFactory.getLogger(TestReconstructStripedFile.class);

  private ErasureCodingPolicy ecPolicy;
  private int dataBlkNum;
  private int parityBlkNum;
  private int cellSize;
  private int blockSize;
  private int groupSize;
  private int dnNum;

  static {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(BlockManager.blockLog, Level.TRACE);
  }

  enum ReconstructionType {
    DataOnly,
    ParityOnly,
    Any
  }

  @Rule
  public TemporaryFolder baseDir = new TemporaryFolder();

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  // Map: DatanodeID -> datanode index in cluster
  private Map<DatanodeID, Integer> dnMap = new HashMap<>();
  private final Random random = new Random();

  private String policyName = "";
  private List<String> dataNodeUUIDs = null;
  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  public boolean isValidationEnabled() {
    return false;
  }

  public int getPendingTimeout() {
    return DFSConfigKeys
            .DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }

  @Before
  public void setup() throws IOException {

    ecPolicy = getEcPolicy();
    dataBlkNum = ecPolicy.getNumDataUnits();
    parityBlkNum = ecPolicy.getNumParityUnits();
    cellSize = ecPolicy.getCellSize();
    blockSize = 5 * 1024 * 1024;
    groupSize = dataBlkNum + parityBlkNum;
    dnNum = groupSize + parityBlkNum;

    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(
            DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
            cellSize - 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
              CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
              NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    conf.setInt(
            DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
            getPendingTimeout());
    conf.setBoolean(DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_VALIDATION_KEY,
            isValidationEnabled());
    cluster = new MiniDFSCluster.Builder(conf, baseDir.getRoot()).numDataNodes(dnNum)
            .build();
    cluster.waitActive();
    random.setSeed(100);

    fs = cluster.getFileSystem();
    // int RS_POLICY_INDEX = 0;
    int TR_POLICY_INDEX = 5;
    String policyName = SystemErasureCodingPolicies.getPolicies().get(TR_POLICY_INDEX).getName();
    fs.enableErasureCodingPolicy(policyName);
    // fs.enableErasureCodingPolicy(ecPolicy.getName());
    fs.getClient().setErasureCodingPolicy("/", policyName);
    // fs.getClient().setErasureCodingPolicy("/", ecPolicy.getName());

    List<DataNode> datanodes = cluster.getDataNodes();
    dataNodeUUIDs = datanodes.stream().map(dataNode -> dataNode.getDatanodeUuid().toString()).collect(Collectors.toList());
    for (int i = 0; i < dnNum; i++) {
      dnMap.put(datanodes.get(i).getDatanodeId(), i);
    }
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    long currentTime = (new Date()).getTime();
  }

  private void printLogInfo() {
    List<DataNode> datanodes = cluster.getDataNodes();
    dataNodeUUIDs = datanodes.stream().map(dataNode -> dataNode.getDatanodeUuid().toString()).collect(Collectors.toList());
  }


  /*@Test(timeout = 120000)
  public void testRecoverOneDataBlockSmallFile2() throws Exception {
    int FILE_LEN_TEST_1 = 2  * 6291456; // 6 * 1024 * 1024: to make the rounding byte
    int fileLen = FILE_LEN_TEST_1; //
    String testCaseName = "12MB-AllZeros";
    ourECLogger = OurECLogger.getInstance(testCaseName);
    ourECLogger.write(this, "TESTCASE - 12MB-AllZeros");
    ourECLogger.write(this, "fileLen: " + fileLen);
    printLogInfo();
    ourTestLogger = OurTestLogger.getInstance(testCaseName);

    assertFileBlocksReconstruction("/testRecoverOneDataBlock", fileLen,
            ReconstructionType.DataOnly, 1);
    // [TODO] Write test code to read the content from the test file and assert
    assertResults(1);
  }
  /*


  @Test(timeout = 120000)
  public void testRecoverOneDataBlockFullStripe() throws Exception {
    // int fileLen = (dataBlkNum + 1) * blockSize + blockSize / 10;
    // int fileLen = (dataBlkNum + parityBlkNum) * cellSize;
    int FILE_LEN_TEST_2 = 3840000;
    int FILE_LEN_TEST_1 = 6291456; // 6 * 64 * 3 * 10000: to make the rounding byte
    int FILE_LEN_TEST_3 = 8 * FILE_LEN_TEST_1; //
    int fileLen = FILE_LEN_TEST_3; //
    String testCaseName = "48MB-AllZeros";
    ourECLogger = OurECLogger.getInstance(testCaseName);
    ourECLogger.write(this, "TESTCASE - 48MB-AllZeros");
    ourECLogger.write(this, "fileLen: " + fileLen);
    printLogInfo();
    ourTestLogger = OurTestLogger.getInstance(testCaseName);

    assertFileBlocksReconstruction("/testRecoverOneDataBlock", fileLen,
            ReconstructionType.DataOnly, 1);
    // [TODO] Write test code to read the content from the test file and assert
    assertResults(1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneParityBlock() throws Exception {
    int fileLen = (dataBlkNum + 1) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverOneParityBlock", fileLen,
        ReconstructionType.ParityOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneParityBlock1() throws Exception {
    int fileLen = cellSize + cellSize / 10;
    assertFileBlocksReconstruction("/testRecoverOneParityBlock1", fileLen,
        ReconstructionType.ParityOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneParityBlock2() throws Exception {
    int fileLen = 1;
    assertFileBlocksReconstruction("/testRecoverOneParityBlock2", fileLen,
        ReconstructionType.ParityOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneParityBlock3() throws Exception {
    int fileLen = (dataBlkNum - 1) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverOneParityBlock3", fileLen,
        ReconstructionType.ParityOnly, 1);
  }*/

  /*@Test(timeout = 120000)
  public void testRecoverAllParityBlocks() throws Exception {
    int fileLen = dataBlkNum * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAllParityBlocks", fileLen,
        ReconstructionType.ParityOnly, parityBlkNum);
  }*/

  /*@Test(timeout = 120000)
  public void testRecoverAllDataBlocks() throws Exception {
    int fileLen = (dataBlkNum + parityBlkNum) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAllDataBlocks", fileLen,
        ReconstructionType.DataOnly, parityBlkNum);
  }

  @Test(timeout = 120000)
  public void testRecoverAllDataBlocks1() throws Exception {
    int fileLen = parityBlkNum * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAllDataBlocks1", fileLen,
        ReconstructionType.DataOnly, parityBlkNum);
  }*/

  /*@Test(timeout = 120000)
  public void testRecoverOneDataBlock() throws Exception {
    int fileLen = (dataBlkNum + 1) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverOneDataBlock", fileLen,
        ReconstructionType.DataOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneDataBlock1() throws Exception {
    int fileLen = cellSize + cellSize/10;
    assertFileBlocksReconstruction("/testRecoverOneDataBlock1", fileLen,
        ReconstructionType.DataOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneDataBlock2() throws Exception {
    int fileLen = 1;
    assertFileBlocksReconstruction("/testRecoverOneDataBlock2", fileLen,
        ReconstructionType.DataOnly, 1);
  }*/

  /*@Test(timeout = 120000)
  public void testRecoverAnyBlocks() throws Exception {
    int fileLen = parityBlkNum * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAnyBlocks", fileLen,
        ReconstructionType.Any, random.nextInt(parityBlkNum) + 1);
  }

  @Test(timeout = 120000)
  public void testRecoverAnyBlocks1() throws Exception {
    int fileLen = (dataBlkNum + parityBlkNum) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAnyBlocks1", fileLen,
        ReconstructionType.Any, random.nextInt(parityBlkNum) + 1);
  }*/
  private int[] generateDeadDnIndices(ReconstructionType type, int deadNum,
                                      byte[] indices) {
    List<Integer> deadList = new ArrayList<>(deadNum);
    while (deadList.size() < deadNum) {
      int dead = random.nextInt(indices.length);
      boolean isOfType = true;
      if (type == ReconstructionType.DataOnly) {
        isOfType = indices[dead] < dataBlkNum;
      } else if (type == ReconstructionType.ParityOnly) {
        isOfType = indices[dead] >= dataBlkNum;
      }
      if (isOfType && !deadList.contains(dead)) {
        deadList.add(dead);
      }
    }
    int[] d = new int[deadNum];
    for (int i = 0; i < deadNum; i++) {
      d[i] = deadList.get(i);
    }
    // [TODO] Clarify this change.
    return d;
  }

  private void shutdownDataNode(DataNode dn) throws IOException {
    /*
     * Kill the datanode which contains one replica
     * We need to make sure it dead in namenode: clear its update time and
     * trigger NN to check heartbeat.
     */
    dn.shutdown();
    cluster.setDataNodeDead(dn.getDatanodeId());
  }

  private int generateErrors(Map<ExtendedBlock, DataNode> corruptTargets,
                             ReconstructionType type)
          throws IOException {
    int stoppedDNs = 0;
    for (Map.Entry<ExtendedBlock, DataNode> target :
            corruptTargets.entrySet()) {
      if (stoppedDNs == 0 || type != ReconstructionType.DataOnly
              || random.nextBoolean()) {
        // stop at least one DN to trigger reconstruction
        LOG.info("Note: stop DataNode " + target.getValue().getDisplayName()
                + " with internal block " + target.getKey());
        shutdownDataNode(target.getValue());
        stoppedDNs++;
      } else { // corrupt the data on the DN
        LOG.info("Note: corrupt data on " + target.getValue().getDisplayName()
                + " with internal block " + target.getKey());
        cluster.corruptReplica(target.getValue(), target.getKey());
      }
    }
    return stoppedDNs;
  }

  /*private void writeFile(DistributedFileSystem fs, String fileName,
                         int fileLen) throws Exception {
    final byte[] data = new byte[fileLen];
    int block10Len = fileLen / 10;
    for (int i = 0; i < block10Len; i++) {
      byte value = (byte) (i % 2 + 10);
      Arrays.fill(data, i * 10, (i + 1) * 10, value);
    }
    Arrays.fill(data, block10Len * 10, fileLen, (byte) 1);
    byte[] subData = Arrays.copyOfRange(data, 0, 60);
    ourECLogger.write(TestReconstructStripedFile.class, "create file - data: " + Arrays.toString(subData));
    DFSTestUtil.writeFile(fs, new Path(fileName), data);
    StripedFileTestUtil.waitBlockGroupsReported(fs, fileName);
  }*/

  private static void writeFile(DistributedFileSystem fs, String fileName,
                                int fileLen) throws Exception {
    final byte[] data = new byte[fileLen];
    // Arrays.fill(data, (byte) 1);
    new Random().nextBytes(data);
    DFSTestUtil.writeFile(fs, new Path(fileName), data);
    StripedFileTestUtil.waitBlockGroupsReported(fs, fileName);
  }

  @Test(timeout = 1200000)
  public void testRecoverOneDataBlockSmallFile() throws Exception {
    // int fileLen = 6 * 1024 * 1024; // 6 * 1024 * 1024: to make the rounding byte
    int fileLen = 6 * blockSize;
    assertFileBlocksReconstructionTraceRepair("/testRecoverOneDataBlock", fileLen,
            ReconstructionType.DataOnly, 1);
  }

  /**
   * Test the file blocks reconstruction.
   * 1. Check the replica is reconstructed in the target datanode,
   *    and verify the block replica length, generationStamp and content.
   * 2. Read the file and verify content.
   */
  private void assertFileBlocksReconstructionTraceRepair(String fileName, int fileLen,
                                                         ReconstructionType type, int toRecoverBlockNum) throws Exception {
    if (toRecoverBlockNum < 1 || toRecoverBlockNum > parityBlkNum) {
      Assert.fail("toRecoverBlockNum should be between 1 ~ " + parityBlkNum);
    }
    assertTrue("File length must be positive.", fileLen > 0);

    // [NOTE] Create a file and write to it in the
    //        filesystem (/tmp). Encode data into
    //        parity blocks.
    Path file = new Path(fileName);
    writeFile(fs, fileName, fileLen);

    // Segment the file into blocks.
    LocatedBlocks locatedBlocks =
            StripedFileTestUtil.getLocatedBlocks(file, fs);
    assertEquals(locatedBlocks.getFileLength(), fileLen);
    // The last block will hold updated info on all blocks?
    LocatedStripedBlock lastBlock =
            (LocatedStripedBlock) locatedBlocks.getLastLocatedBlock();
    DatanodeInfo[] storageInfos = lastBlock.getLocations();
    byte[] indices = lastBlock.getBlockIndices();

    BitSet bitset = new BitSet(dnNum);
    for (DatanodeInfo storageInfo : storageInfos) {
      bitset.set(dnMap.get(storageInfo));
    }
    // This has been hardcoded to 1.
    // int[] dead = generateDeadDnIndices(type, toRecoverBlockNum, indices);
    int[] dead = new int[] {1};

    double Debug_FileLengthKB = fileLen / (1024 * 1024 * 1.0);

    DatanodeInfo[] dataDNs = new DatanodeInfo[toRecoverBlockNum];
    int[] deadDnIndices = new int[toRecoverBlockNum];
    ExtendedBlock[] blocks = new ExtendedBlock[toRecoverBlockNum];
    File[] replicas = new File[toRecoverBlockNum];
    long[] replicaLengths = new long[toRecoverBlockNum];
    File[] metaDatas = new File[toRecoverBlockNum];
    byte[][] replicaContents = new byte[toRecoverBlockNum][];
    Map<ExtendedBlock, DataNode> errorMap = new HashMap<>(dead.length);
    // [DEBUG] To recover block num is 1. Gets the data on the dead node.
    for (int i = 0; i < toRecoverBlockNum; i++) {
      dataDNs[i] = storageInfos[dead[i]];
      deadDnIndices[i] = dnMap.get(dataDNs[i]);

      // Check the block replica file on deadDn before it died.
      blocks[i] = StripedBlockUtil.constructInternalBlock(
              lastBlock.getBlock(), cellSize, dataBlkNum, indices[dead[i]]);
      errorMap.put(blocks[i], cluster.getDataNodes().get(deadDnIndices[i]));
      replicas[i] = cluster.getBlockFile(deadDnIndices[i], blocks[i]);
      replicaLengths[i] = replicas[i].length();
      metaDatas[i] = cluster.getBlockMetadataFile(deadDnIndices[i], blocks[i]);
      // The block replica on the datanode should be the same as expected.
      assertEquals(replicaLengths[i],
              StripedBlockUtil.getInternalBlockLength(
                      lastBlock.getBlockSize(), cellSize, dataBlkNum, indices[dead[i]]));
      assertTrue(metaDatas[i].getName().
              endsWith(blocks[i].getGenerationStamp() + ".meta"));

      // [DEBUG] Logging.
      LOG.info("replica " + i + " locates in file: " + replicas[i]);
      replicaContents[i] = DFSTestUtil.readFileAsBytes(replicas[i]);
      byte[] subReplicaContents = Arrays.copyOfRange(replicaContents[i], 0, 40);
    }

    int lastGroupDataLen = fileLen % (dataBlkNum * blockSize);
    int lastGroupNumBlk = lastGroupDataLen == 0 ? dataBlkNum :
            Math.min(dataBlkNum, ((lastGroupDataLen - 1) / cellSize + 1));
    int groupSize = lastGroupNumBlk + parityBlkNum;

    // Shutdown datanodes or generate corruption.
    int stoppedDN = generateErrors(errorMap, type);

    // Check the locatedBlocks of the file again.
    locatedBlocks = StripedFileTestUtil.getLocatedBlocks(file, fs);
    lastBlock = (LocatedStripedBlock) locatedBlocks.getLastLocatedBlock();
    storageInfos = lastBlock.getLocations();
    assertEquals(storageInfos.length, groupSize - stoppedDN);

    int[] targetDNs = new int[dnNum - groupSize];
    int n = 0;
    for (int i = 0; i < dnNum; i++) {
      if (!bitset.get(i)) { // not contain replica of the block.
        targetDNs[n++] = i;
      }
    }

    // [DEBUG] Logging.
    for (int i = 0; i < toRecoverBlockNum; i++) {
      DataNode dataNode = cluster.getDataNodes().get(deadDnIndices[i]);
    }

    StripedFileTestUtil.waitForReconstructionFinished(file, fs, groupSize);

    targetDNs = sortTargetsByReplicas(blocks, targetDNs);

    // Check the replica on the new target node.
    for (int i = 0; i < toRecoverBlockNum; i++) {
      File replicaAfterReconstruction = cluster.getBlockFile(targetDNs[i], blocks[i]);
      File metadataAfterReconstruction =
              cluster.getBlockMetadataFile(targetDNs[i], blocks[i]);
      assertEquals(replicaLengths[i], replicaAfterReconstruction.length());
      assertTrue(metadataAfterReconstruction.getName().
              endsWith(blocks[i].getGenerationStamp() + ".meta"));
      byte[] replicaContentAfterReconstruction =
              DFSTestUtil.readFileAsBytes(replicaAfterReconstruction);

      boolean isRecovering = Arrays.equals(replicaContents[i], replicaContentAfterReconstruction);
      if (!isRecovering) {
        byte[] subArrayBeforeReplica = Arrays.copyOfRange(replicaContents[i], 0, 60);

        byte[] subArrayAfterReplicaContent = Arrays.copyOfRange(replicaContentAfterReconstruction, 0, 60);
      }
      Assert.assertArrayEquals(replicaContents[i], replicaContentAfterReconstruction);
    }
  }

  // [TODO] Can this be combined with the above? Note, this is
  //        by another test case {@link TestReconstructStripedFileWithValidator}.
  void assertFileBlocksReconstruction(String fileName, int fileLen,
                                      ReconstructionType type, int toRecoverBlockNum) throws Exception {
    if (toRecoverBlockNum < 1 || toRecoverBlockNum > parityBlkNum) {
      Assert.fail("toRecoverBlockNum should be between 1 ~ " + parityBlkNum);
    }
    assertTrue("File length must be positive.", fileLen > 0);

    Path file = new Path(fileName);

    writeFile(fs, fileName, fileLen);

    LocatedBlocks locatedBlocks =
            StripedFileTestUtil.getLocatedBlocks(file, fs);
    assertEquals(locatedBlocks.getFileLength(), fileLen);

    LocatedStripedBlock lastBlock =
            (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();

    DatanodeInfo[] storageInfos = lastBlock.getLocations();
    byte[] indices = lastBlock.getBlockIndices();

    BitSet bitset = new BitSet(dnNum);
    for (DatanodeInfo storageInfo : storageInfos) {
      bitset.set(dnMap.get(storageInfo));
    }

    int[] dead = generateDeadDnIndices(type, toRecoverBlockNum, indices);
    LOG.info("Note: indices == " + Arrays.toString(indices)
            + ". Generate errors on datanodes: " + Arrays.toString(dead));

    DatanodeInfo[] dataDNs = new DatanodeInfo[toRecoverBlockNum];
    int[] deadDnIndices = new int[toRecoverBlockNum];
    ExtendedBlock[] blocks = new ExtendedBlock[toRecoverBlockNum];
    File[] replicas = new File[toRecoverBlockNum];
    long[] replicaLengths = new long[toRecoverBlockNum];
    File[] metadatas = new File[toRecoverBlockNum];
    byte[][] replicaContents = new byte[toRecoverBlockNum][];
    Map<ExtendedBlock, DataNode> errorMap = new HashMap<>(dead.length);
    for (int i = 0; i < toRecoverBlockNum; i++) {
      dataDNs[i] = storageInfos[dead[i]];
      deadDnIndices[i] = dnMap.get(dataDNs[i]);

      // Check the block replica file on deadDn before it dead.
      blocks[i] = StripedBlockUtil.constructInternalBlock(
              lastBlock.getBlock(), cellSize, dataBlkNum, indices[dead[i]]);
      errorMap.put(blocks[i], cluster.getDataNodes().get(deadDnIndices[i]));
      replicas[i] = cluster.getBlockFile(deadDnIndices[i], blocks[i]);
      replicaLengths[i] = replicas[i].length();
      metadatas[i] = cluster.getBlockMetadataFile(deadDnIndices[i], blocks[i]);
      // the block replica on the datanode should be the same as expected
      assertEquals(replicaLengths[i],
              StripedBlockUtil.getInternalBlockLength(
                      lastBlock.getBlockSize(), cellSize, dataBlkNum, indices[dead[i]]));
      assertTrue(metadatas[i].getName().
              endsWith(blocks[i].getGenerationStamp() + ".meta"));
      LOG.info("replica " + i + " locates in file: " + replicas[i]);
      replicaContents[i] = DFSTestUtil.readFileAsBytes(replicas[i]);
    }

    int lastGroupDataLen = fileLen % (dataBlkNum * blockSize);
    int lastGroupNumBlk = lastGroupDataLen == 0 ? dataBlkNum :
            Math.min(dataBlkNum, ((lastGroupDataLen - 1) / cellSize + 1));
    int groupSize = lastGroupNumBlk + parityBlkNum;

    // shutdown datanodes or generate corruption
    int stoppedDN = generateErrors(errorMap, type);

    // Check the locatedBlocks of the file again
    locatedBlocks = StripedFileTestUtil.getLocatedBlocks(file, fs);
    lastBlock = (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
    storageInfos = lastBlock.getLocations();
    assertEquals(storageInfos.length, groupSize - stoppedDN);

    int[] targetDNs = new int[dnNum - groupSize];
    int n = 0;
    for (int i = 0; i < dnNum; i++) {
      if (!bitset.get(i)) { // not contain replica of the block.
        targetDNs[n++] = i;
      }
    }

    StripedFileTestUtil.waitForReconstructionFinished(file, fs, groupSize);

    targetDNs = sortTargetsByReplicas(blocks, targetDNs);

    // Check the replica on the new target node.
    for (int i = 0; i < toRecoverBlockNum; i++) {
      File replicaAfterReconstruction = cluster.getBlockFile(targetDNs[i], blocks[i]);
      LOG.info("replica after reconstruction " + replicaAfterReconstruction);
      File metadataAfterReconstruction =
              cluster.getBlockMetadataFile(targetDNs[i], blocks[i]);
      assertEquals(replicaLengths[i], replicaAfterReconstruction.length());
      LOG.info("replica before " + replicas[i]);
      assertTrue(metadataAfterReconstruction.getName().
              endsWith(blocks[i].getGenerationStamp() + ".meta"));
      byte[] replicaContentAfterReconstruction =
              DFSTestUtil.readFileAsBytes(replicaAfterReconstruction);

      Assert.assertArrayEquals(replicaContents[i], replicaContentAfterReconstruction);
    }
  }

  private int[] sortTargetsByReplicas(ExtendedBlock[] blocks, int[] targetDNs) {
    int[] result = new int[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      result[i] = -1;
      for (int j = 0; j < targetDNs.length; j++) {
        if (targetDNs[j] != -1) {
          File replica = cluster.getBlockFile(targetDNs[j], blocks[i]);
          if (replica != null) {
            result[i] = targetDNs[j];
            targetDNs[j] = -1;
            break;
          }
        }
      }
      if (result[i] == -1) {
        Assert.fail("Failed to reconstruct striped block: "
                + blocks[i].getBlockId());
      }
    }
    return result;
  }

  // [TODO] Should these be disabled?
  /*
   * Tests that processErasureCodingTasks should not throw exceptions out due to
   * invalid ECTask submission.
   */
  /*@Test
  public void testProcessErasureCodingTasksSubmitionShouldSucceed()
      throws Exception {
    DataNode dataNode = cluster.dataNodes.get(0).datanode;

    // Pack invalid(dummy) parameters in ecTasks. Irrespective of parameters, each task
    // thread pool submission should succeed, so that it will not prevent
    // processing other tasks in the list if any exceptions.
    int size = cluster.dataNodes.size();
    byte[] liveIndices = new byte[size];
    DatanodeInfo[] dataDNs = new DatanodeInfo[size + 1];
    DatanodeStorageInfo targetDnInfos_1 = BlockManagerTestUtil
        .newDatanodeStorageInfo(DFSTestUtil.getLocalDatanodeDescriptor(),
            new DatanodeStorage("s01"));
    DatanodeStorageInfo[] dnStorageInfo = new DatanodeStorageInfo[] {
        targetDnInfos_1 };

    BlockECReconstructionInfo invalidECInfo = new BlockECReconstructionInfo(
        new ExtendedBlock("bp-id", 123456), dataDNs, dnStorageInfo, liveIndices,
        new byte[0], ecPolicy);
    List<BlockECReconstructionInfo> ecTasks = new ArrayList<>();
    ecTasks.add(invalidECInfo);
    dataNode.getErasureCodingWorker().processErasureCodingTasks(ecTasks);
  }

  // HDFS-12044
  @Test(timeout = 120000)
  public void testNNSendsErasureCodingTasks() throws Exception {
    testNNSendsErasureCodingTasks(1);
    testNNSendsErasureCodingTasks(2);
  }

  private void testNNSendsErasureCodingTasks(int deadDN) throws Exception {
    cluster.shutdown();

    final int numDataNodes = dnNum  + 1;
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 20);
    conf.setInt(DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_THREADS_KEY,
        2);
    // Set shorter socket timeout, to allow the recovery task to be reschedule,
    // if it is connecting to a dead DataNode.
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 5 * 1000);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    ErasureCodingPolicy policy = ecPolicy;
    fs.enableErasureCodingPolicy(policy.getName());
    fs.getClient().setErasureCodingPolicy("/", policy.getName());

    final int fileLen = cellSize * ecPolicy.getNumDataUnits();
    for (int i = 0; i < 50; i++) {
      writeFile(fs, "/ec-file-" + i, fileLen);
    }

    // Inject data-loss by tear down desired number of DataNodes.
    assumeTrue("Ignore case where num dead DNs > num parity units",
        policy.getNumParityUnits() >= deadDN);
    List<DataNode> dataNodes = new ArrayList<>(cluster.getDataNodes());
    Collections.shuffle(dataNodes);
    for (DataNode dn : dataNodes.subList(0, deadDN)) {
      shutdownDataNode(dn);
    }

    final FSNamesystem ns = cluster.getNamesystem();
    GenericTestUtils.waitFor(() -> ns.getPendingDeletionBlocks() == 0,
        500, 30000);

    // Make sure that all pending reconstruction tasks can be processed.
    while (ns.getPendingReconstructionBlocks() > 0) {
      long timeoutPending = ns.getNumTimedOutPendingReconstructions();
      assertEquals(String
          .format("Found %d timeout pending reconstruction tasks",
              timeoutPending), 0, timeoutPending);
      Thread.sleep(1000);
    }

    // Verify all DN reaches zero xmitsInProgress.
    GenericTestUtils.waitFor(() ->
        cluster.getDataNodes().stream().mapToInt(
            DataNode::getXmitsInProgress).sum() == 0,
        500, 30000
    );
  }

  @Test(timeout = 180000)
  public void testErasureCodingWorkerXmitsWeight() throws Exception {
    testErasureCodingWorkerXmitsWeight(0.5f,
        (int) (ecPolicy.getNumDataUnits() * 0.5f));
    testErasureCodingWorkerXmitsWeight(1f, ecPolicy.getNumDataUnits());
    testErasureCodingWorkerXmitsWeight(0f, 1);
    testErasureCodingWorkerXmitsWeight(10f, 10 * ecPolicy.getNumDataUnits());
  }

  private void testErasureCodingWorkerXmitsWeight(
      float weight, int expectedWeight)
      throws Exception {

    // Reset cluster with customized xmits weight.
    conf.setFloat(DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_XMITS_WEIGHT_KEY,
        weight);
    cluster.shutdown();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(dnNum).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    fs.getClient().setErasureCodingPolicy("/", ecPolicy.getName());

    final int fileLen = cellSize * ecPolicy.getNumDataUnits() * 2;
    writeFile(fs, "/ec-xmits-weight", fileLen);

    DataNode dn = cluster.getDataNodes().get(0);
    int corruptBlocks = dn.getFSDataset().getFinalizedBlocks(
        cluster.getNameNode().getNamesystem().getBlockPoolId()).size();
    int expectedXmits = corruptBlocks * expectedWeight;

    final CyclicBarrier barrier = new CyclicBarrier(corruptBlocks + 1);
    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector delayInjector = new DataNodeFaultInjector() {
      public void stripedBlockReconstruction() throws IOException {
        try {
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          throw new IOException(e);
        }
      }
    };
    DataNodeFaultInjector.set(delayInjector);

    try {
      shutdownDataNode(dn);
      LambdaTestUtils.await(30 * 1000, 500,
          () -> {
            int totalXmits = cluster.getDataNodes().stream()
                  .mapToInt(DataNode::getXmitsInProgress).sum();
            return totalXmits == expectedXmits;
          }
      );
    } finally {
      barrier.await();
      DataNodeFaultInjector.set(oldInjector);
      for (final DataNode curDn : cluster.getDataNodes()) {
        GenericTestUtils.waitFor(() -> curDn.getXceiverCount() <= 1, 10, 60000);
        GenericTestUtils.waitFor(() -> curDn.getXmitsInProgress() == 0, 10,
            2500);
      }
    }
  }*/

  /**
   * When the StripedBlockReader timeout, the outdated future should be ignored.
   * Or the NPE will be thrown, which will stop reading the remaining data, and
   * the reconstruction task will fail.
   */
  @Test(timeout = 120000)
  public void testTimeoutReadBlockInReconstruction() throws Exception {
    assumeTrue("Ignore case where num parity units <= 1",
            ecPolicy.getNumParityUnits() > 1);
    int stripedBufferSize = conf.getInt(
            DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
            cellSize);
    ErasureCodingPolicy policy = ecPolicy;
    fs.enableErasureCodingPolicy(policy.getName());
    fs.getClient().setErasureCodingPolicy("/", policy.getName());

    // StripedBlockReconstructor#reconstruct will loop 2 times
    final int fileLen = stripedBufferSize * 2 * ecPolicy.getNumDataUnits();
    String fileName = "/timeout-read-block";
    Path file = new Path(fileName);
    writeFile(fs, fileName, fileLen);
    fs.getFileBlockLocations(file, 0, fileLen);

    LocatedBlocks locatedBlocks =
            StripedFileTestUtil.getLocatedBlocks(file, fs);
    Assert.assertEquals(1, locatedBlocks.getLocatedBlocks().size());
    // The file only has one block group
    LocatedBlock lblock = locatedBlocks.get(0);
    DatanodeInfo[] datanodeinfos = lblock.getLocations();

    // to reconstruct first block
    DataNode dataNode = cluster.getDataNode(datanodeinfos[0].getIpcPort());

    int stripedReadTimeoutInMills = conf.getInt(
            DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY,
            DFSConfigKeys.
                    DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_DEFAULT);
    Assert.assertTrue(
            DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY
                    + " must be greater than 2000",
            stripedReadTimeoutInMills > 2000);

    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector timeoutInjector = new DataNodeFaultInjector() {
      private AtomicInteger numDelayReader = new AtomicInteger(0);

      @Override
      public void delayBlockReader() {
        int index = numDelayReader.incrementAndGet();
        LOG.info("Delay the {}th read block", index);

        // the file's first StripedBlockReconstructor#reconstruct,
        // and the first reader will timeout
        if (index == 1) {
          try {
            GenericTestUtils.waitFor(() -> numDelayReader.get() >=
                            ecPolicy.getNumDataUnits() + 1, 50,
                    stripedReadTimeoutInMills * 3
            );
          } catch (TimeoutException e) {
            Assert.fail("Can't reconstruct the file's first part.");
          } catch (InterruptedException e) {
          }
        }
        // stop all the following re-reconstruction tasks
        if (index > 3 * ecPolicy.getNumDataUnits() + 1) {
          while (true) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
          }
        }
      }
    };
    DataNodeFaultInjector.set(timeoutInjector);

    try {
      shutdownDataNode(dataNode);
      // before HDFS-15240, NPE will cause reconstruction fail(test timeout)
      StripedFileTestUtil
              .waitForReconstructionFinished(file, fs, groupSize);
    } finally {
      DataNodeFaultInjector.set(oldInjector);
    }
  }

  /**
   * When block reader timeout, the outdated future should be ignored.
   * Or the ByteBuffer would be wrote after giving back to the BufferPool.
   * This UT is used to ensure that we should close block reader
   * before freeing the buffer.
   */
  @Test(timeout = 120000)
  public void testAbnormallyCloseDoesNotWriteBufferAgain() throws Exception {
    assumeTrue("Ignore case where num parity units <= 1",
            ecPolicy.getNumParityUnits() > 1);
    int stripedBufferSize = conf.getInt(
            DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
            cellSize);
    // StripedBlockReconstructor#reconstruct will loop 2 times
    final int fileLen = stripedBufferSize * 2 * ecPolicy.getNumDataUnits();
    String fileName = "/no-dirty-buffer";
    Path file = new Path(fileName);
    writeFile(fs, fileName, fileLen);
    fs.getFileBlockLocations(file, 0, fileLen);

    LocatedBlocks locatedBlocks =
            StripedFileTestUtil.getLocatedBlocks(file, fs);
    Assert.assertEquals(1, locatedBlocks.getLocatedBlocks().size());
    // The file only has one block group
    LocatedBlock lblock = locatedBlocks.get(0);
    DatanodeInfo[] datanodeinfos = lblock.getLocations();

    // to reconstruct first block
    DataNode dataNode = cluster.getDataNode(datanodeinfos[0].getIpcPort());

    int stripedReadTimeoutInMills = conf.getInt(
            DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY,
            DFSConfigKeys.
                    DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_DEFAULT);
    Assert.assertTrue(
            DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY
                    + " must be greater than 2000",
            stripedReadTimeoutInMills > 2000);

    ElasticByteBufferPool bufferPool =
            (ElasticByteBufferPool) ErasureCodingTestHelper.getBufferPool();
    emptyBufferPool(bufferPool, true);
    emptyBufferPool(bufferPool, false);

    AtomicInteger finishedReadBlock = new AtomicInteger(0);

    DataNodeFaultInjector oldInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector timeoutInjector = new DataNodeFaultInjector() {
      private AtomicInteger numDelayReader = new AtomicInteger(0);
      private AtomicBoolean continueRead = new AtomicBoolean(false);
      private AtomicBoolean closeByNPE = new AtomicBoolean(false);

      @Override
      public void delayBlockReader() {
        int index = numDelayReader.incrementAndGet();
        LOG.info("Delay the {}th read block", index);

        // the file's first StripedBlockReconstructor#reconstruct,
        // and the first reader will timeout
        if (index == 1) {
          try {
            GenericTestUtils.waitFor(() -> numDelayReader.get() >=
                            ecPolicy.getNumDataUnits() + 1, 50,
                    stripedReadTimeoutInMills * 3
            );
          } catch (TimeoutException e) {
            Assert.fail("Can't reconstruct the file's first part.");
          } catch (InterruptedException e) {
          }
        }
        if (index > ecPolicy.getNumDataUnits() + 1) {
          try {
            GenericTestUtils.waitFor(
                    () -> {
                      LOG.info("Close by NPE: {}, continue read: {}",
                              closeByNPE, continueRead);
                      return closeByNPE.get() ? continueRead.get()
                              : index == finishedReadBlock.get() + 1; }, 5,
                    stripedReadTimeoutInMills * 3
            );
          } catch (TimeoutException e) {
            Assert.fail("Can't reconstruct the file's remaining part.");
          } catch (InterruptedException e) {
          }
        }
      }

      @Override
      public void interceptBlockReader() {
        int n = finishedReadBlock.incrementAndGet();
        LOG.info("Intercept the end of {}th read block.", n);
      }

      private AtomicInteger numFreeBuffer = new AtomicInteger(0);
      @Override
      public void interceptFreeBlockReaderBuffer() {
        closeByNPE.compareAndSet(false, true);
        int num = numFreeBuffer.incrementAndGet();
        LOG.info("Intercept the {} free block buffer.", num);
        if (num >= ecPolicy.getNumDataUnits() + 1) {
          continueRead.compareAndSet(false, true);
          try {
            GenericTestUtils.waitFor(() -> finishedReadBlock.get() >=
                            2 * ecPolicy.getNumDataUnits() + 1, 50,
                    stripedReadTimeoutInMills * 3
            );
          } catch (TimeoutException e) {
            Assert.fail("Can't finish the file's reconstruction.");
          } catch (InterruptedException e) {
          }
        }
      }
    };
    DataNodeFaultInjector.set(timeoutInjector);
    try {
      shutdownDataNode(dataNode);
      // at least one timeout reader
      GenericTestUtils.waitFor(() -> finishedReadBlock.get() >=
                      2 * ecPolicy.getNumDataUnits() + 1, 50,
              stripedReadTimeoutInMills * 3
      );

      assertBufferPoolIsEmpty(bufferPool, false);
      assertBufferPoolIsEmpty(bufferPool, true);
      StripedFileTestUtil.waitForReconstructionFinished(file, fs, groupSize);
    } finally {
      DataNodeFaultInjector.set(oldInjector);
    }
  }

  private void assertBufferPoolIsEmpty(ElasticByteBufferPool bufferPool,
                                       boolean direct) {
    while (bufferPool.size(direct) != 0) {
      // iterate all ByteBuffers in ElasticByteBufferPool
      ByteBuffer byteBuffer =  bufferPool.getBuffer(direct, 0);
      Assert.assertEquals(0, byteBuffer.position());
    }
  }

  private void emptyBufferPool(ElasticByteBufferPool bufferPool,
                               boolean direct) {
    while (bufferPool.size(direct) != 0) {
      bufferPool.getBuffer(direct, 0);
    }
  }
}
