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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.RecoveryTable;
import org.apache.hadoop.io.erasurecode.rawcoder.InvalidDecodingException;
import org.apache.hadoop.util.MetricTimer;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timeline;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * StripedBlockReconstructor reconstruct one or more missed striped block in
 * the striped block group, the minimum number of live striped blocks should
 * be no less than data block number.
 */
@InterfaceAudience.Private
class StripedBlockReconstructor extends StripedReconstructor
        implements Runnable {
  private final RecoveryTable recoveryTable;
  private final int nodeCount = getStripedReader().numberOfInputs();
  ByteBuffer[] totalByteBuffers = new ByteBuffer[nodeCount];
  private StripedWriter stripedWriter;
  private boolean isTR = false;
  CollectChunkStream reconstructTargetInputs;

  StripedBlockReconstructor(ErasureCodingWorker worker,
                            StripedReconstructionInfo stripedReconInfo) {
    super(worker, stripedReconInfo);
    int totalBlkNum = stripedReconInfo.getEcPolicy().getNumDataUnits() + stripedReconInfo.getEcPolicy().getNumParityUnits();
    this.recoveryTable = new RecoveryTable(totalBlkNum);

    stripedWriter = new StripedWriter(this, getDatanode(),
            getConf(), stripedReconInfo);

    if (stripedReconInfo.getEcPolicy().getCodecName().equals("tr")) {
      isTR = true;
    }
  }

  boolean hasValidTargets() {
    return stripedWriter.hasValidTargets();
  }

  @Override
  public void run() {
    MetricTimer metricTimer = new MetricTimer(Thread.currentThread().getId());
    metricTimer.start("Recovery");
    Timeline.mark("START", "Recovery", Thread.currentThread().getId());
    try {
      initDecoderIfNecessary();
      initDecodingValidatorIfNecessary();
      getStripedReader().init();
      stripedWriter.init();
      reconstruct();
      stripedWriter.endTargetBlocks();
      // Currently we don't check the acks for packets, this is similar as
      // block replication.
    } catch (Throwable e) {
      LOG.warn("Failed to reconstruct striped block: {}", getBlockGroup(), e);
      getDatanode().getMetrics().incrECFailedReconstructionTasks();
    } finally {
      float xmitWeight = getErasureCodingWorker().getXmitWeight();
      // if the xmits is smaller than 1, the xmitsSubmitted should be set to 1
      // because if it set to zero, we cannot to measure the xmits submitted
      int xmitsSubmitted = Math.max((int) (getXmits() * xmitWeight), 1);
      getDatanode().decrementXmitsInProgress(xmitsSubmitted);
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionTasks();
      metrics.incrECReconstructionBytesRead(getBytesRead());
      metrics.incrECReconstructionRemoteBytesRead(getRemoteBytesRead());
      metrics.incrECReconstructionBytesWritten(getBytesWritten());

      getStripedReader().close();
      stripedWriter.close();
      cleanup();
    }
    Timeline.mark("END", "Recovery", Thread.currentThread().getId());
    metricTimer.end("Recovery");
  }

  @Override
  void reconstruct() throws IOException {
    if (isTR) {
      int erasedNodeIndex = getStripedReader().getErasedIndex();
      reconstructTargetInputs = new CollectChunkStream(nodeCount, getMaxTargetLength(), erasedNodeIndex, recoveryTable);
    }
    MetricTimer metricTimer = new MetricTimer(Thread.currentThread().getId());
    while (getPositionInBlock() < getMaxTargetLength()) {
      DataNodeFaultInjector.get().stripedBlockReconstruction();
      long remaining = getMaxTargetLength() - getPositionInBlock();
      final int toReconstructLen =
              (int) Math.min(getStripedReader().getBufferSize(), remaining);

      long start = Time.monotonicNow();
      long bytesToRead = (long) toReconstructLen * getStripedReader().getMinRequiredSources();
      if (getDatanode().getEcReconstuctReadThrottler() != null) {
        getDatanode().getEcReconstuctReadThrottler().throttle(bytesToRead);
      }
      // step1: read from minimum source DNs required for reconstruction.
      // The returned success list is the source DNs we do real read from
      getStripedReader().readMinimumSources(toReconstructLen);
      long readEnd = Time.monotonicNow();

      // step2: decode to reconstruct targets
      Timeline.mark("START", "Reconstruct", Thread.currentThread().getId());
      metricTimer.start("Reconstruct");
      if (isTR) {
        reconstructTraces(toReconstructLen);
      } else {
        reconstructTargets(toReconstructLen);
      }
      metricTimer.end("Reconstruct");
      Timeline.mark("END", "Reconstruct", Thread.currentThread().getId());
      long decodeEnd = Time.monotonicNow();

      // step3: transfer data
      long bytesToWrite = (long) toReconstructLen * stripedWriter.getTargets();
      if (getDatanode().getEcReconstuctWriteThrottler() != null) {
        getDatanode().getEcReconstuctWriteThrottler().throttle(bytesToWrite);
      }
      if (stripedWriter.transferData2Targets() == 0) {
        String error = "Transfer failed for all targets.";
        throw new IOException(error);
      }
      long writeEnd = Time.monotonicNow();

      // Only successful reconstructions are recorded.
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionReadTime(readEnd - start);
      metrics.incrECReconstructionDecodingTime(decodeEnd - readEnd);
      metrics.incrECReconstructionWriteTime(writeEnd - decodeEnd);
      updatePositionInBlock(toReconstructLen);

      clearBuffers();
    }
  }

  private void reconstructTargets(int toReconstructLen) throws IOException {
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);
    int[] erasedIndices = stripedWriter.getRealTargetIndices();
    ByteBuffer[] outputs = stripedWriter.getRealTargetBuffers(toReconstructLen);

    if (isValidationEnabled()) {
      markBuffers(inputs);
      decode(inputs, erasedIndices, outputs);
      resetBuffers(inputs);

      DataNodeFaultInjector.get().badDecoding(outputs);
      long start = Time.monotonicNow();
      try {
        getValidator().validate(inputs, erasedIndices, outputs);
        long validateEnd = Time.monotonicNow();
        getDatanode().getMetrics().incrECReconstructionValidateTime(
                validateEnd - start);
      } catch (InvalidDecodingException e) {
        long validateFailedEnd = Time.monotonicNow();
        getDatanode().getMetrics().incrECReconstructionValidateTime(
                validateFailedEnd - start);
        getDatanode().getMetrics().incrECInvalidReconstructionTasks();
        throw e;
      }
    } else {
      decode(inputs, erasedIndices, outputs);
    }
    stripedWriter.updateRealTargetBuffers(toReconstructLen);
  }

  private void reconstructTraces(int toReconstructLen) throws IOException {
    MetricTimer metricTimer = new MetricTimer(Thread.currentThread().getId());

    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);
    int[] erasedIndices = stripedWriter.getRealTargetIndices();
    ByteBuffer[] outputs = stripedWriter.getRealTargetBuffers(toReconstructLen);

    metricTimer.start("Collect chunks");
    reconstructTargetInputs.appendInputs(inputs);
    ByteBuffer[] decoderInputs = reconstructTargetInputs.getInputs(toReconstructLen);
    metricTimer.end("Collect chunks");
        
    decode(decoderInputs, erasedIndices, outputs);
    stripedWriter.updateRealTargetBuffers(toReconstructLen);
  }
  
  private void decode(ByteBuffer[] inputs, int[] erasedIndices,
                      ByteBuffer[] outputs) throws IOException {
    long start = System.nanoTime();
    getDecoder().decode(inputs, erasedIndices, outputs);
    long end = System.nanoTime();
    this.getDatanode().getMetrics().incrECDecodingTime(end - start);
  }

  /**
   * Clear all associated buffers.
   */
  private void clearBuffers() {
    getStripedReader().clearBuffers();
    stripedWriter.clearBuffers();
  }


  String bandwidths() {
    byte[] bandwidth = new byte[nodeCount];
    int erasedNodeIndex = getStripedReader().getErasedIndex();
    for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
      bandwidth[nodeIndex] = recoveryTable.getByte(nodeIndex, erasedNodeIndex, 0);
    }
    return Arrays.toString(bandwidth);
  }
  
  static class CollectChunkStream {
    byte[] bandwidth;
    int nodeCount;
    int erasedNodeIndex;
    RecoveryTable recoveryTable;
    int[] bufferReadPointers;
    int[] writeLengths;
    byte[][] totalInputs;

    CollectChunkStream(int nodeCount, long maxTargetLength, int erasedNodeIndex, RecoveryTable recoveryTable) {
      this.totalInputs = new byte[nodeCount][(int) maxTargetLength];
      this.writeLengths = new int[nodeCount];
      this.nodeCount = nodeCount;
      this.erasedNodeIndex = erasedNodeIndex;
      this.recoveryTable = recoveryTable;
      this.bufferReadPointers = new int[nodeCount];
      this.bandwidth = new byte[nodeCount];
      for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        bandwidth[nodeIndex] = recoveryTable.getByte(nodeIndex, erasedNodeIndex, 0);
      }
    }
    void appendInputs(ByteBuffer[] receivedByteBuffers) {
      assert nodeCount == receivedByteBuffers.length;

      for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        if (nodeIndex == erasedNodeIndex) { continue; }
        assert receivedByteBuffers[nodeIndex] != null;
        receivedByteBuffers[nodeIndex].rewind();
        int nodeBufferLength = receivedByteBuffers[nodeIndex].capacity();
        receivedByteBuffers[nodeIndex].get(totalInputs[nodeIndex], writeLengths[nodeIndex], nodeBufferLength);
        writeLengths[nodeIndex] += nodeBufferLength;
      }
    }

    ByteBuffer[] getInputs(int toReconstructLen) {
      ByteBuffer[] decoderInputs = new ByteBuffer[nodeCount];
      for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        if (nodeIndex == erasedNodeIndex) { continue; }
        int inputLimit = toReconstructLen * bandwidth[nodeIndex] / 8;

        byte[] trimmedInput = new byte[inputLimit];
        System.arraycopy(totalInputs[nodeIndex], bufferReadPointers[nodeIndex], trimmedInput, 0, inputLimit);
        decoderInputs[nodeIndex] = ByteBuffer.wrap(trimmedInput);
        bufferReadPointers[nodeIndex] = bufferReadPointers[nodeIndex] + inputLimit;
      }
      return decoderInputs;
    }
  }
}
