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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient.CorruptedBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.BlockReadStats;
import org.apache.hadoop.hdfs.util.StripedBlockUtil.StripingChunkReadResult;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.impl.HelperTable96Client;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

/**
 * Manage striped readers that performs reading of block data from remote to
 * serve input data for the erasure decoding.
 */
@InterfaceAudience.Private
class StripedReader {
  private static OurECLogger ourECLogger = OurECLogger.getInstance();
  private static final Logger LOG = DataNode.LOG;

  private final int stripedReadTimeoutInMills;
  private final int stripedReadBufferSize;

  private StripedReconstructor reconstructor;
  private final DataNode datanode;
  private final Configuration conf;

  private final int dataBlkNum;
  private final int parityBlkNum;
  private final int totalBlkNum;

  private boolean isTr = false;

  private DataChecksum checksum;
  // Striped read buffer size
  private int bufferSize;
  private int[] successList;

  private final int minRequiredSources;
  // the number of xmits used by the re-construction task.
  private final int xmits;
  // The buffers and indices for striped blocks whose length is 0
  private ByteBuffer[] zeroStripeBuffers;
  private short[] zeroStripeIndices;

  private int erasedIndex;
  // sources
  private final byte[] liveIndices;
  private final DatanodeInfo[] sources;

  private final List<StripedBlockReader> readers;

  private final Map<Future<BlockReadStats>, Integer> futures = new HashMap<>();
  private final CompletionService<BlockReadStats> readService;

  private final BitSet liveBitSet;
  private final ErasureCodingPolicy ecPolicy;
  private HelperTable96Client helperTable = new HelperTable96Client();

  MetricTimer inboundTrafficTimer;

  StripedReader(StripedReconstructor reconstructor, DataNode datanode,
      Configuration conf, StripedReconstructionInfo stripedReconInfo) {
    stripedReadTimeoutInMills = conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_TIMEOUT_MILLIS_DEFAULT);
    stripedReadBufferSize = conf.getInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_DEFAULT);

    this.reconstructor = reconstructor;
    this.datanode = datanode;
    this.conf = conf;
    
    inboundTrafficTimer = TimerFactory.getTimer("Inbound_Traffic");

    dataBlkNum = stripedReconInfo.getEcPolicy().getNumDataUnits();
    parityBlkNum = stripedReconInfo.getEcPolicy().getNumParityUnits();
    totalBlkNum = dataBlkNum + parityBlkNum;

    int cellsNum = (int) ((stripedReconInfo.getBlockGroup().getNumBytes() - 1)
        / stripedReconInfo.getEcPolicy().getCellSize() + 1);

    // [TODO] Clean.
    this.ecPolicy = stripedReconInfo.getEcPolicy();
    liveBitSet = new BitSet(
            ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits());
    for (int i = 0; i < stripedReconInfo.getLiveIndices().length; i++) {
      liveBitSet.set(stripedReconInfo.getLiveIndices()[i]);
    }
    if (stripedReconInfo.getEcPolicy().getCodecName().equals("tr")) {
      this.isTr = true;

      //minRequiredSources = Math.min(cellsNum, (totalBlkNum - 1));
      minRequiredSources = totalBlkNum - 1;
      if (minRequiredSources < (totalBlkNum - 1)) {
        int zeroStripNum = (totalBlkNum - 1) - minRequiredSources;
        zeroStripeBuffers = new ByteBuffer[zeroStripNum];
        zeroStripeIndices = new short[zeroStripNum];
      }
    } else {
      minRequiredSources = Math.min(cellsNum, dataBlkNum);
      if (minRequiredSources < dataBlkNum) {
        int zeroStripNum = dataBlkNum - minRequiredSources;
        zeroStripeBuffers = new ByteBuffer[zeroStripNum];
        zeroStripeIndices = new short[zeroStripNum];
      }
    }
    // It is calculated by the maximum number of connections from either sources
    // or targets.
    xmits = Math.max(minRequiredSources,
        stripedReconInfo.getTargets() != null ?
        stripedReconInfo.getTargets().length : 0);

    this.liveIndices = stripedReconInfo.getLiveIndices();
    assert liveIndices != null;
    this.sources = stripedReconInfo.getSources();
    assert sources != null;

    readers = new ArrayList<>(sources.length);
    readService = reconstructor.createReadService();

    Preconditions.checkArgument(liveIndices.length >= minRequiredSources,
        "No enough live striped blocks.");
    Preconditions.checkArgument(liveIndices.length == sources.length,
        "liveBlockIndices and source datanodes should match");
  }

  void init() throws IOException {
    initReaders();

    initBufferSize();

    initZeroStrip();
  }

  private void initReaders() throws IOException {
    // Store the array indices of source DNs we have read successfully.
    // In each iteration of read, the successList list may be updated if
    // some source DN is corrupted or slow. And use the updated successList
    // list of DNs for next iteration read.
    successList = new int[minRequiredSources];

    StripedBlockReader reader;
    int nSuccess = 0;
    // [TODO] Clean.
    if (this.isTr) {
      //stripedReconInfo.getEcPolicy().getCodecName()
      for (int j = 0; j < totalBlkNum; j++) {
        if (!liveBitSet.get(j)) {
          erasedIndex = j;
          break;
        }
      }

      for (int i = 0; i < sources.length && nSuccess < minRequiredSources; i++) {

        //we know failed node index erasedIndex
        //if i < j, then call createReader(i)
        //if i >= j, then call createReader(i+1)
        //this logic excludes the erasedIndex and creates readers with only live node indices
        int j = i; //to store idxInSources
        if (i < erasedIndex)
          reader = createTRReader(j, i, 0);
        else
          reader = createTRReader(j, i + 1, 0);
        readers.add(reader);
        if (reader.getBlockReader() != null) {
          initOrVerifyChecksum(reader);
          successList[nSuccess++] = i;
        }
      }
    } else {
      for (int i = 0; i < sources.length && nSuccess < minRequiredSources; i++) {
        reader = createReader(i, 0);
        readers.add(reader);
        if (reader.getBlockReader() != null) {
          initOrVerifyChecksum(reader);
          successList[nSuccess++] = i;
        }
      }
    }
    if (nSuccess < minRequiredSources) {
      String error = "Can't find minimum sources required by "
          + "reconstruction, block id: "
          + reconstructor.getBlockGroup().getBlockId();
      throw new IOException(error);
    }
  }

  StripedBlockReader createReader(int idxInSources, long offsetInBlock) {
    return new StripedBlockReader(this, datanode,
        conf, liveIndices[idxInSources],
        reconstructor.getBlock(liveIndices[idxInSources]),
        sources[idxInSources], offsetInBlock, reconstructor.getBlockGroup());
  }

  StripedBlockReader createTRReader(int idxInSources, int helperIndex, long offsetInBlock) {
    return new StripedBlockReader(this, datanode,
            conf, liveIndices[idxInSources],
            reconstructor.getBlock(liveIndices[idxInSources]),
            sources[idxInSources], offsetInBlock,
            isTr, helperIndex, erasedIndex, dataBlkNum, parityBlkNum, reconstructor.getBlockGroup());
  }

  private int numberOfChunks(long datalen) {
    return (int) ((datalen + DFSUtilClient.CHUNK_SIZE - 1)/DFSUtilClient.CHUNK_SIZE);
  }

  private void initBufferSize() {
    // [TODO] Clean.
    int bytesPerChecksum = checksum.getBytesPerChecksum();
    // The bufferSize is flat to divide bytesPerChecksum
    int readBufferSize = stripedReadBufferSize;
    // bufferSize = readBufferSize < bytesPerChecksum ? bytesPerChecksum :
    //     readBufferSize - readBufferSize % bytesPerChecksum;
    bufferSize = DFSUtilClient.CHUNK_SIZE * numberOfChunks(DFSUtilClient.getIoFileBufferSize(conf));
  }

  // init checksum from block reader
  private void initOrVerifyChecksum(StripedBlockReader reader) {
    if (checksum == null) {
      checksum = reader.getBlockReader().getDataChecksum();
    } else {
      assert reader.getBlockReader().getDataChecksum().equals(checksum);
    }
  }

  protected ByteBuffer allocateReadBuffer() {
    return reconstructor.allocateBuffer(getBufferSize());
  }

  private void initZeroStrip() {
    if (zeroStripeBuffers != null) {
      for (int i = 0; i < zeroStripeBuffers.length; i++) {
        zeroStripeBuffers[i] = reconstructor.allocateBuffer(bufferSize);
      }
    }

    BitSet bitset = reconstructor.getLiveBitSet();
    int k = 0;
    for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
      if (!bitset.get(i)) {
        if (reconstructor.getBlockLen(i) <= 0) {
          zeroStripeIndices[k++] = (short)i;
        }
      }
    }
  }

  private int getReadLength(int index, int reconstructLength) {
    // the reading length should not exceed the length for reconstruction
    long blockLen = reconstructor.getBlockLen(index);
    long remaining = blockLen - reconstructor.getPositionInBlock();
    ourECLogger.write(this, datanode.getDatanodeUuid(), "getReadLength - index: " + index + " - blockLen: " +
            blockLen + " - remaining: " + remaining + " - reconstructLength: " + reconstructLength);
    return (int) Math.min(remaining, reconstructLength);
  }

  public int getErasedIndex() {
    return erasedIndex;
  }

  public int numberOfInputs() {
    return dataBlkNum + parityBlkNum;
  }

  ByteBuffer[] getInputBuffers(int toReconstructLen) {
    ByteBuffer[] inputs = new ByteBuffer[dataBlkNum + parityBlkNum];

    for (int i = 0; i < successList.length; i++) {
      int index = successList[i];
      StripedBlockReader reader = getReader(index);
      int readerIndex = reader.getIndex();

      ByteBuffer buffer = reader.getReadBuffer();
      paddingBufferToLen(buffer, toReconstructLen);
      inputs[readerIndex] = (ByteBuffer)buffer.flip();
      ByteBuffer input = inputs[readerIndex];
      byte[] tempBuffer = input.array();

      int helperNodeIndex = readerIndex;
      Object element = helperTable.getElement(helperNodeIndex, erasedIndex);
      String s = element.toString();
      String[] elements = s.split(",");
      int traceBandwidth = Integer.parseInt(elements[0]);
      int ONE_BYTE = 8; // 8 bits.
      int tempBytesToKeep = (tempBuffer.length) * traceBandwidth / ONE_BYTE;
      if (readerIndex == 3) {
        ourECLogger.write(this, datanode.getDatanodeUuid(), "getInputBuffers 0 - reader.getIndex(): " + readerIndex + " - " +
                " - dataLen: " + tempBuffer.length + " - dataLenToKeep: " + tempBytesToKeep + " - data: " +
                Arrays.toString(Arrays.copyOfRange(tempBuffer, 0, tempBuffer.length)));
       // ourECLogger.write(this, datanode.getDatanodeUuid(), "getInputBuffers 0 - reader.getIndex(): " + readerIndex + " - " +
       //         " - dataLen: " + tempBuffer.length + " - dataLenToKeep: " + tempBytesToKeep + " - data: " +
       //         Arrays.toString(Arrays.copyOfRange(tempBuffer, 0, 100)) +
       //         "- endData: " + Arrays.toString(Arrays.copyOfRange(tempBuffer, tempBytesToKeep - 30, tempBytesToKeep)));
      }
    }

    if (successList.length < dataBlkNum) {
      for (int i = 0; i < zeroStripeBuffers.length; i++) {
        ByteBuffer buffer = zeroStripeBuffers[i];
        paddingBufferToLen(buffer, toReconstructLen);
        int index = zeroStripeIndices[i];
        inputs[index] = (ByteBuffer)buffer.flip();
      }
    }
    return inputs;
  }

  private void paddingBufferToLen(ByteBuffer buffer, int len) {
    if (len > buffer.limit()) {
      buffer.limit(len);
    }
    int toPadding = len - buffer.position();
    for (int i = 0; i < toPadding; i++) {
      buffer.put((byte) 0);
    }
  }

  /**
   * Read from minimum source DNs required for reconstruction in the iteration.
   * First try the success list which we think they are the best DNs
   * If source DN is corrupt or slow, try to read some other source DN,
   * and will update the success list.
   *
   * Remember the updated success list and return it for following
   * operations and next iteration read.
   *
   * @param reconstructLength the length to reconstruct.
   * @return updated success list of source DNs we do real read
   * @throws IOException
   */
  void readMinimumSources(int reconstructLength) throws IOException {
    CorruptedBlocks corruptedBlocks = new CorruptedBlocks();
    try {
      successList = doReadMinimumSources(reconstructLength, corruptedBlocks);
    } finally {
      // report corrupted blocks to NN
      datanode.reportCorruptedBlocks(corruptedBlocks);
    }
  }

  int[] doReadMinimumSources(int reconstructLength,
                             CorruptedBlocks corruptedBlocks)
      throws IOException {
    Preconditions.checkArgument(reconstructLength >= 0 &&
        reconstructLength <= bufferSize);
    int nSuccess = 0;
    int[] newSuccess = new int[minRequiredSources];
    BitSet usedFlag = new BitSet(sources.length);
    /*
     * Read from minimum source DNs required, the success list contains
     * source DNs which we think best.
     */
    ourECLogger.write(this, datanode.getDatanodeUuid(), "successList: " + Arrays.toString(successList));
    ourECLogger.write(this, datanode.getDatanodeUuid(), "liveIndices: " + Arrays.toString(liveIndices));

    DatanodeID datanodeID = null;
    
    for (int i = 0; i < minRequiredSources; i++) {
      StripedBlockReader reader = readers.get(successList[i]);

      if (isTr) {
        datanodeID = reader.getBlockTraceReaderRemote().getDatanodeID();
      } else {
        datanodeID = reader.getBlockReaderRemote().getDatanodeID();
      }

      int toRead = getReadLength(liveIndices[successList[i]],
          reconstructLength);
      ourECLogger.write(this, datanode.getDatanodeUuid(), "toRead: " + toRead + " - liveIndex: " + liveIndices[successList[i]]);
      inboundTrafficTimer.mark("Block\t" + reconstructor.getBlockGroup().getBlockId() + "\tSource:\t" + datanodeID.getXferAddr() + "\tLength\t" + toRead);
      if (toRead > 0) {
        Callable<BlockReadStats> readCallable =
            reader.readFromBlock(toRead, corruptedBlocks);
        Future<BlockReadStats> f = readService.submit(readCallable);
        futures.put(f, successList[i]);
      } else {
        // If the read length is 0, we don't need to do real read
        // [TODO] Machine index is ignored.
        reader.getReadBuffer().position(0);
        newSuccess[nSuccess++] = successList[i];
      }
      usedFlag.set(successList[i]);
    }
    ourECLogger.write(this, datanode.getDatanodeUuid(), "nSuccess: " + nSuccess + " - newSuccess: " + Arrays.toString(newSuccess));
    ourECLogger.write(this, datanode.getDatanodeUuid(), "futures size: " + futures.keySet().size() + " - futures: " + futures);

    while (!futures.isEmpty()) {
      try {
        StripingChunkReadResult result =
            StripedBlockUtil.getNextCompletedStripedRead(
                readService, futures, stripedReadTimeoutInMills);
        int resultIndex = -1;
        ourECLogger.write(this, datanode.getDatanodeUuid(), "result index: " + result.index + " - state: " + result.state);
        ourECLogger.write(this, datanode.getDatanodeUuid(), "nSuccess: " + nSuccess + "newSuccess: " + Arrays.toString(newSuccess));

        if (result.state == StripingChunkReadResult.SUCCESSFUL) {
          resultIndex = result.index;
        } else if (result.state == StripingChunkReadResult.FAILED) {
          // If read failed for some source DN, we should not use it anymore
          // and schedule read from another source DN.
          StripedBlockReader failedReader = readers.get(result.index);
          failedReader.closeBlockReader();
          ourECLogger.write(this, datanode.getDatanodeUuid(), "closeBlockReader 1: " + result.index);

          resultIndex = scheduleNewRead(usedFlag,
              reconstructLength, corruptedBlocks);
          ourECLogger.write(this, datanode.getDatanodeUuid(), "closeBlockReader 2: " + resultIndex);

        } else if (result.state == StripingChunkReadResult.TIMEOUT) {
          ourECLogger.write(this, datanode.getDatanodeUuid(), "timeout 1: " + result.index);

          // If timeout, we also schedule a new read.
          resultIndex = scheduleNewRead(usedFlag,
              reconstructLength, corruptedBlocks);
          ourECLogger.write(this, datanode.getDatanodeUuid(), "timeout 1: " + resultIndex);

        }
        if (resultIndex >= 0) {
          newSuccess[nSuccess++] = resultIndex;
          if (nSuccess >= minRequiredSources) {
            // cancel remaining reads if we read successfully from minimum
            // number of source DNs required by reconstruction.
            cancelReads(futures.keySet());
            clearFuturesAndService();
            break;
          }
        }
        ourECLogger.write(this, datanode.getDatanodeUuid(), "nSuccess: " + nSuccess + "newSuccess: " + Arrays.toString(newSuccess));

      } catch (InterruptedException e) {
        LOG.info("Read data interrupted.", e);
        cancelReads(futures.keySet());
        clearFuturesAndService();
        break;
      }
    }
    ourECLogger.write(this, datanode.getDatanodeUuid(), "complete - nSuccess / minRequiredSources: " + nSuccess + "/" + minRequiredSources);
    if (nSuccess < minRequiredSources) {
      String error = "Can't read data from minimum number of sources "
          + "required by reconstruction, block id: " +
          reconstructor.getBlockGroup().getBlockId();
      throw new IOException(error);
    }
    MetricTimer endRead = TimerFactory.getTimer("Completed_Striped_Read");
    endRead.mark(reconstructor.getBlockGroup().getBlockId() + "\tEnd Read");
    
    return newSuccess;
  }

  /**
   * Schedule a read from some new source DN if some DN is corrupted
   * or slow, this is called from the read iteration.
   * Initially we may only have <code>minRequiredSources</code> number of
   * StripedBlockReader.
   * If the position is at the end of target block, don't need to do
   * real read, and return the array index of source DN, otherwise -1.
   *
   * @param used the used source DNs in this iteration.
   * @return the array index of source DN if don't need to do real read.
   */
  private int scheduleNewRead(BitSet used, int reconstructLength,
                              CorruptedBlocks corruptedBlocks) {
    StripedBlockReader reader = null;
    // step1: initially we may only have <code>minRequiredSources</code>
    // number of StripedBlockReader, and there may be some source DNs we never
    // read before, so will try to create StripedBlockReader for one new source
    // DN and try to read from it. If found, go to step 3.
    int m = readers.size();
    int toRead = 0;
    while (reader == null && m < sources.length) {
      reader = createReader(m, reconstructor.getPositionInBlock());
      readers.add(reader);
      toRead = getReadLength(liveIndices[m], reconstructLength);
      if (toRead > 0) {
        if (reader.getBlockReader() == null) {
          reader = null;
          m++;
        }
      } else {
        used.set(m);
        return m;
      }
    }

    // step2: if there is no new source DN we can use, try to find a source
    // DN we ever read from but because some reason, e.g., slow, it
    // is not in the success DN list at the begin of this iteration, so
    // we have not tried it in this iteration. Now we have a chance to
    // revisit it again.
    for (int i = 0; reader == null && i < readers.size(); i++) {
      if (!used.get(i)) {
        StripedBlockReader stripedReader = readers.get(i);
        toRead = getReadLength(liveIndices[i], reconstructLength);
        if (toRead > 0) {
          stripedReader.closeBlockReader();
          stripedReader.resetBlockReader(reconstructor.getPositionInBlock());
          ourECLogger.write(this, datanode.getDatanodeUuid(), "scheduleNewRead- close StripedReader");
          if (stripedReader.getBlockReader() != null) {
            // [TODO] Index is ignored.
            stripedReader.getReadBuffer().position(0);
            m = i;
            reader = stripedReader;
          }
        } else {
          used.set(i);
          // [TODO] Index is ignored.
          stripedReader.getReadBuffer().position(0);
          return i;
        }
      }
    }

    // step3: schedule if find a correct source DN and need to do real read.
    if (reader != null) {
      Callable<BlockReadStats> readCallable =
          reader.readFromBlock(toRead, corruptedBlocks);
      Future<BlockReadStats> f = readService.submit(readCallable);
      futures.put(f, m);
      used.set(m);
    }

    return -1;
  }

  // Cancel all reads.
  private static void cancelReads(Collection<Future<BlockReadStats>> futures) {
    for (Future<BlockReadStats> future : futures) {
      future.cancel(true);
    }
  }

  // remove all stale futures from readService, and clear futures.
  private void clearFuturesAndService() {
    while (!futures.isEmpty()) {
      try {
        Future<BlockReadStats> future = readService.poll(
            stripedReadTimeoutInMills, TimeUnit.MILLISECONDS
        );
        futures.remove(future);
      } catch (InterruptedException e) {
        LOG.info("Clear stale futures from service is interrupted.", e);
      }
    }
  }

  void close() {
    ourECLogger.write(this, datanode.getDatanodeUuid(), "close all BlockReader: " + readers);

    if (zeroStripeBuffers != null) {
      for (ByteBuffer zeroStripeBuffer : zeroStripeBuffers) {
        reconstructor.freeBuffer(zeroStripeBuffer);
      }
    }
    zeroStripeBuffers = null;

    for (StripedBlockReader reader : readers) {
      reader.closeBlockReader();
      // [TODO] Index is ignored.
      reconstructor.freeBuffer(reader.getReadBuffer());
      reader.freeReadBuffer();
    }
  }

  StripedReconstructor getReconstructor() {
    return reconstructor;
  }

  StripedBlockReader getReader(int i) {
    return readers.get(i);
  }

  int getBufferSize() {
    return bufferSize;
  }

  DataChecksum getChecksum() {
    return checksum;
  }

  void clearBuffers() {
    if (zeroStripeBuffers != null) {
      for (ByteBuffer zeroStripeBuffer : zeroStripeBuffers) {
        zeroStripeBuffer.clear();
      }
    }

    for (StripedBlockReader reader : readers) {
      if (reader.getReadBuffer() != null) {
        reader.getReadBuffer().clear();
      }
    }
  }

  InetSocketAddress getSocketAddress4Transfer(DatanodeInfo dnInfo) {
    return reconstructor.getSocketAddress4Transfer(dnInfo);
  }

  CachingStrategy getCachingStrategy() {
    return reconstructor.getCachingStrategy();
  }

  /**
   * Return the xmits of this EC reconstruction task.
   * <p>
   * DN uses it to coordinate with NN to adjust the speed of scheduling the
   * EC reconstruction tasks to this DN.
   *
   * @return the xmits of this reconstruction task.
   */
  int getXmits() {
    return xmits;
  }

  public int getMinRequiredSources() {
    return minRequiredSources;
  }
}
