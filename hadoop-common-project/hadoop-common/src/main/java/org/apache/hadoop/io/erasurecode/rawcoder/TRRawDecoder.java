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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.RecoveryTable;
import org.apache.hadoop.util.MetricTimer;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.DualBasisTable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A raw decoder of the Trace Repair code scheme in pure Java.
 *
 */

@InterfaceAudience.Private
public class TRRawDecoder extends RawErasureDecoder {
    public TRRawDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
        preCompute();
        this.recoveryTable = new RecoveryTable(coderOptions.getNumAllUnits());
        this.bw = new byte[coderOptions.getNumAllUnits()];
        this.dualBasisTable = new DualBasisTable(coderOptions.getNumAllUnits());
    }

    private final RecoveryTable recoveryTable;

    private final DualBasisTable dualBasisTable;

    private byte[] preComputedParity = new byte[256];

    private byte[] bw;

    byte[] masks = {(byte) 0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};

    @Override
    public synchronized void decode(ByteBuffer[] inputs, int[] erasedIndexes,
                                    ByteBuffer[] outputs) throws IOException {
        ByteBufferDecodingState decodingState = new ByteBufferDecodingState(this,
                inputs, erasedIndexes, outputs, true);

        boolean usingDirectBuffer = decodingState.usingDirectBuffer;
        int dataLen = decodingState.decodeLength;
        if (dataLen == 0) {
            return;
        }

        int[] inputPositions = new int[inputs.length];
        for (int i = 0; i < inputPositions.length; i++) {
            if (inputs[i] != null) {
                inputPositions[i] = inputs[i].position();
            }
        }

        if (usingDirectBuffer) {
            doDecode(decodingState);
        } else {
            ByteArrayDecodingState badState = decodingState.convertToByteArrayState();
            doDecode(badState);
        }

        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i] != null) {
                // dataLen bytes consumed
                inputs[i].position(inputPositions[i] + (int) Math.ceil(dataLen * bw[i] / 8.0));
            }
        }
    }

    @Override
    protected void doDecode(ByteBufferDecodingState decodingState) {
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.decodeLength);
        throw new RuntimeException("Not tested yet.");  // [DEBUG]
    }

    @Override
    protected void doDecode(ByteArrayDecodingState decodingState) {
        MetricTimer metricTimer = new MetricTimer(Thread.currentThread().getId());
        CoderUtil.resetOutputBuffers(
            decodingState.outputs,
            decodingState.outputOffsets,
            decodingState.decodeLength
        );
        // [FIXME] This should be done over every erased node.
        int erasedIndex = decodingState.erasedIndexes[0];
        int n = decodingState.decoder.getNumAllUnits();

        // [WARN] We assume only one node fails.
        byte[][] binaryTraces = new byte[n][];
        // [NOTE] Calculate bandwidth.
        for (int i = 0; i < n; i++) {
            bw[i] = recoveryTable.getByte(i, erasedIndex, 0);
        }
        metricTimer.start("Decompress trace");
        byte[] decimalTrace = decompressTraceCombined(
                decodingState.inputs, decodingState.inputOffsets,
                erasedIndex, decodingState.decodeLength
        );
        metricTimer.end("Decompress trace");
        byte[] revMem = repairDecimalTrace(erasedIndex);
        constructCj(
                erasedIndex, decodingState.decodeLength, decimalTrace,
                revMem, decodingState.outputs[0], decodingState.outputOffsets[0]);

        /*reconstructionTimer.start();
        // [NOTE] Decompress traces into its binary format.
        //        The erased trace is not sent over.
        for (int i = 0; i < n; i++) {
            if (i == erasedIdx) { continue; }
            binaryTraces[i] = decompressTrace(
                decodingState.inputs[i], decodingState.inputOffsets[i],
                bw[i] * decodingState.decodeLength);
        }
        reconstructionTimer.stop("Decompress traces");

        // MetricTimer reconstructionTimer = TimerFactory.getTimer("Recovery_Reconstruct");
        reconstructionTimer.start();
        byte[] decimalTrace = convertToDecimalTrace(
            binaryTraces, erasedIdx, decodingState.decodeLength, n);
        reconstructionTimer.stop("Convert to decimal trace");
        reconstructionTimer.start();
        byte[] revMem = repairDecimalTrace(n, erasedIdx);
        reconstructionTimer.stop("Repair decimal trace");
        reconstructionTimer.start();
        constructCj(
            n, erasedIdx, decodingState.decodeLength, decimalTrace,
            revMem, decodingState.outputs[0], decodingState.outputOffsets[0]);  // [WARN] We assume only one node fails.
        reconstructionTimer.stop("Construct Cj");*/
    }

    public byte[] decompressTraceCombined(
            byte[][] compressedTrace,
            int[] inputOffset,
            int erasedIndex,
            int decodeLength
    ) {
        byte[] repairTrace = new byte[getNumAllUnits() * decodeLength];

        for (int nodeIndex = 0; nodeIndex < getNumAllUnits(); nodeIndex++) {
            if (nodeIndex == erasedIndex) { continue; }
            int offset = inputOffset[nodeIndex];
            int idx = nodeIndex * decodeLength;
            int bandwidth = bw[nodeIndex];
            int numBitsToRead = decodeLength * bandwidth;

            if (bandwidth == 4) {
                for (byte b : compressedTrace[nodeIndex]) {
                    byte res1 = (byte) ((b >> 4) & 0x0F);
                    byte res2 = (byte) (b & 0x0F);

                    repairTrace[idx++] = res1;
                    repairTrace[idx++] = res2;
                }
            } else if (bandwidth == 2) {
                for (byte b : compressedTrace[nodeIndex]) {
                    byte first = (byte) ((b >> 4) & 0x0F);
                    byte res1 = (byte) ((first >> 2) & 0x03);
                    byte res2 = (byte) (first & 0x03);
                    byte second = (byte) (b & 0x0F);
                    byte res3 = (byte) ((second >> 2) & 0x03);
                    byte res4 = (byte) (second & 0x03);

                    repairTrace[idx++] = res1;
                    repairTrace[idx++] = res2;
                    repairTrace[idx++] = res3;
                    repairTrace[idx++] = res4;
                }
            } else if (bandwidth == 6) {
                for (int byteIndex = 0; byteIndex < compressedTrace[nodeIndex].length; byteIndex += 3) {
                    byte b1 = compressedTrace[nodeIndex][byteIndex];
                    byte b2 = compressedTrace[nodeIndex][byteIndex + 1];
                    byte b3 = compressedTrace[nodeIndex][byteIndex + 2];

                    byte res_1 = (byte) ((b1 >> 2) & 0x3F);
                    byte res_2 = (byte) (((b1 & 0x03) << 4) | ((b2 >> 4) & 0x0F));
                    byte res_3 = (byte) (((b2 & 0x0F) << 2) | ((b3 >> 6) & 0x03));
                    byte res_4 = (byte) (b3 & 0x3F);

                    repairTrace[idx++] = res_1;
                    repairTrace[idx++] = res_2;
                    repairTrace[idx++] = res_3;
                    repairTrace[idx++] = res_4;
                }
            } else {
                for (int codeWordStart = 0; codeWordStart < numBitsToRead; codeWordStart += bandwidth) {
                    int decimalToConstruct = 0;
                    for (int a = 0; a < bandwidth; a++) {
                        int bitIndex = codeWordStart + a;
                        int byteIndex = bitIndex >> 3;
                        int bitPos = bitIndex & 7;
                        // byte mask = (byte) (1 << (7 - bitPos));
                        byte mask = masks[bitPos];
                        int bit = (byte) (
                                (compressedTrace[nodeIndex][byteIndex + offset] & mask) >> (7 - bitPos) & 1);
                        // (compressedTrace[nodeIndex][byteIndex + offset] & mask) != 0 ? 1 : 0);
                        decimalToConstruct = decimalToConstruct << 1;
                        decimalToConstruct ^= bit;
                    }
                    repairTrace[idx++] = (byte) decimalToConstruct;
                }
            }
        }
        return repairTrace;
    }

    protected byte[] repairDecimalTrace(int erasedIdx) {
        byte[] revMem = new byte[getNumAllUnits() * 256];
        byte[] erasedDualBasis = dualBasisTable.getRow(erasedIdx);
        for (int i = 0; i < getNumAllUnits(); i++) {
            if (i != erasedIdx) {
                byte[] R = recoveryTable.getRow(i, erasedIdx);
                byte[] Rij = new byte[R.length - 1];
                System.arraycopy(R, 1, Rij, 0, Rij.length);
                for (int b = 0; b < 256; b++) {
                    for (int a = 0; a < 8; a++) {
                        int parityIndex = Rij[a] & (byte) (b);
                        int xorResult = (preComputedParity[parityIndex]) * erasedDualBasis[a];
                        revMem[(i<<8) + b] ^= (byte) xorResult;
                    }
                }
            }
        }
        return revMem;
    }

    /*protected byte[] repairDecimalTrace(int n, int erasedIdx) {
        // [NOTE] This should happen on a chunk by chunk basis.
        byte[] revMem = new byte[n * 256];
        byte[] erasedDualBasis = dualBasisTable.getRow_9_6(erasedIdx);
        for (int i = 0; i < n; i++) {
            if (i != erasedIdx) {
                byte[] R = recoveryTable.getRow_9_6(i, erasedIdx);
                byte[] Rij = new byte[R.length - 1];
                System.arraycopy(R, 1, Rij, 0, Rij.length);
                for (int b = 0; b < 256; b++) {
                    for (int a = 0; a < 8; a++) {
                        int parityIndex = Rij[a] & (byte) (b);
                        int xorResult = (preComputedParity[parityIndex]) * erasedDualBasis[a];
                        assert(xorResult >= -127 && xorResult <= 128);
                        revMem[(i<<8) + b] ^= (byte) xorResult;
                    }
                }
            }
        }
        return revMem;
    }*/

    private void constructCj(
            int erasedIdx,
            int decodeLength,
            byte[] decimalTrace,
            byte[] revMem,
            byte[] output,
            int outputOffset
    ) {
        // [NOTE] Traces (inputs) should not involve any erased nodes.
        for (int i = 0; i < getNumAllUnits(); i++) {
            // [NOTE] The traces should not include the erased nodes.
            if (i == erasedIdx) { continue; }
            for (int test_codeword = 0; test_codeword < decodeLength; test_codeword++) {
                int traceIndex = i * decodeLength + test_codeword;
                byte traces_as_number = decimalTrace[traceIndex];
                output[outputOffset + test_codeword]
                        = (byte) (output[outputOffset + test_codeword] ^ revMem[(i << 8) + traces_as_number]);;
            }
        }
    }

    /*private void constructCj(
        int n,
        int erasedIdx,
        int decodeLength,
        byte[] decimalTrace,
        byte[] revMem,
        byte[] output,
        int outputOffset
    ) {
        // [NOTE] Traces (inputs) should not involve any erased nodes.
        for (int i = 0; i < n; i++) {
            // [NOTE] The traces should not include the erased nodes.
            if (i == erasedIdx) { continue; }
            for (int test_codeword = 0; test_codeword < decodeLength; test_codeword++) {
                // [TODO] Handle multi-node failures.
                int traceIndex = i * decodeLength + test_codeword;
                byte traces_as_number = decimalTrace[traceIndex];
                output[outputOffset + test_codeword]
                    = (byte) (output[outputOffset + test_codeword] ^ revMem[(i << 8) + traces_as_number]);;
            }
        }
    }*/

    public static byte[] decompressTrace(byte[] compressedTrace, int inputOffset, int numBitsToRead) {
        byte[] decompressedTrace = new byte[numBitsToRead];
        for (int bitIndex = 0; bitIndex < numBitsToRead; bitIndex++) {
            int byteIndex = bitIndex / 8;
            int bitPos = bitIndex % 8;
            byte mask = (byte) (1 << (7 - bitPos));
            decompressedTrace[bitIndex] = (byte) ((compressedTrace[byteIndex + inputOffset] & mask) != 0 ? 1 : 0);
        }
        return decompressedTrace;
    }

    private byte[] convertToDecimalTrace(byte[][] traces, int erasedIdx, int decodeLength, int n) {
        byte[] decimalTrace = new byte[n * decodeLength];

        // int inputIndex = -1;
        for (int nodeIndex = 0; nodeIndex < n; nodeIndex++) {
            if (nodeIndex == erasedIdx) { continue; }
            // inputIndex++;  // [FIXME] Erased trace is null. Used to index traces.
            int idx = nodeIndex * decodeLength;
            // [NOTE] This is done because we skip the erased node nodeIndex and
            //        traces array does not hold traces of erased nodes.
            for (int test_codeword = 0; test_codeword < decodeLength; test_codeword++) {
                // [WARN] ISAL implementation has traces_as_number as a uint.
                int traces_as_number = 0;
                for (int a = 0; a < bw[nodeIndex]; a++) {
                    traces_as_number = traces_as_number << 1;
                    byte valueToXOR = traces[nodeIndex][a * decodeLength + test_codeword];
                    traces_as_number ^= valueToXOR;
                }
                decimalTrace[idx++] = (byte) traces_as_number;
            }
        }
        return decimalTrace;
    }

    private void preCompute() {
        int i;
        preComputedParity[0] = 0;
        for (i = 1; i < 256; i++) {
            preComputedParity[i] = (byte) (preComputedParity[i >> 1] ^ (i & 1));
        }
    }
}
