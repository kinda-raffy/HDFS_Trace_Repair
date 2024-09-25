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
import org.apache.hadoop.hdfs.DFSUtilClient;
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
  private final RecoveryTable recoveryTable = new RecoveryTable();
  private final int nodeCount = getStripedReader().numberOfInputs();
  ByteBuffer[] totalByteBuffers = new ByteBuffer[nodeCount];
  private StripedWriter stripedWriter;
  private boolean isTR = false;

  StripedBlockReconstructor(ErasureCodingWorker worker,
                            StripedReconstructionInfo stripedReconInfo) {
    super(worker, stripedReconInfo);

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
    MetricTimer timer = new MetricTimer(Thread.currentThread().getId());
    Timeline.mark("START\tRecovery");
    timer.start("recovery");
    try {
      initDecoderIfNecessary();
      initDecodingValidatorIfNecessary();
      getStripedReader().init();
      stripedWriter.init();
      timer.start("reconstruct");
      reconstruct();
      timer.end("reconstruct");
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
    timer.end("recovery");
    Timeline.mark("END\tRecovery");
  }

  @Override
  void reconstruct() throws IOException {
    MetricTimer timer = new MetricTimer(Thread.currentThread().getId());
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
      timer.start("consume_buffer");
      getStripedReader().readMinimumSources(toReconstructLen);
      timer.end("consume_buffer");
      long readEnd = Time.monotonicNow();

      // step2: decode to reconstruct targets
      Timeline.mark("START\tReconstruct");
      reconstructTargets(toReconstructLen);
      Timeline.mark("END\tReconstruct");
      long decodeEnd = Time.monotonicNow();

      // step3: transfer data
      long bytesToWrite = (long) toReconstructLen * stripedWriter.getTargets();
      if (getDatanode().getEcReconstuctWriteThrottler() != null) {
        getDatanode().getEcReconstuctWriteThrottler().throttle(bytesToWrite);
      }
      timer.start("write");
      if (stripedWriter.transferData2Targets() == 0) {
        String error = "Transfer failed for all targets.";
        throw new IOException(error);
      }
      timer.end("write");
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

    // Validation is not tested to work
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
      if (isTR) {
        int erasedNodeIndex = getStripedReader().getErasedIndex();
        
        CollectChunkStream reconstructTargetInputs = new CollectChunkStream(nodeCount, DFSUtilClient.CHUNK_SIZE, erasedNodeIndex, recoveryTable);
        reconstructTargetInputs.appendInputs(inputs);
        ByteBuffer[] decoderInputs = reconstructTargetInputs.getInputs(toReconstructLen);
        
        decode(decoderInputs, erasedIndices, outputs);
      } else {
        decode(inputs, erasedIndices, outputs);
      }
    }
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
      bandwidth[nodeIndex] = recoveryTable.getByte_9_6(nodeIndex, erasedNodeIndex, 0);
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

    CollectChunkStream(int nodeCount, int chunkSize, int erasedNodeIndex, RecoveryTable recoveryTable) {
      this.totalInputs = new byte[nodeCount][chunkSize];
      this.writeLengths = new int[nodeCount];
      this.nodeCount = nodeCount;
      this.erasedNodeIndex = erasedNodeIndex;
      this.recoveryTable = recoveryTable;
      this.bufferReadPointers = new int[nodeCount];
      this.bandwidth = new byte[nodeCount];
      for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        bandwidth[nodeIndex] = recoveryTable.getByte_9_6(nodeIndex, erasedNodeIndex, 0);
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
