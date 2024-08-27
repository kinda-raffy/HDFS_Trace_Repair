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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.HelperTable;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.util.MetricTimer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A raw erasure encoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible. This new Java coder is about 5X faster than the one originated
 * from HDFS-RAID, and also compatible with the native/ISA-L coder.
 */
@InterfaceAudience.Private
public class TRRawEncoder extends RawErasureEncoder {
    // relevant to schema and won't change during encode calls.
    private byte[] encodeMatrix;

    private byte[] gfTables;

    private byte[] preComputedParity = new byte[256];

    private final HelperTable helperTable = new HelperTable();

    public TRRawEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            throw new HadoopIllegalArgumentException(
                    "Invalid numDataUnits and numParityUnits");
        }
        encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
        RSUtil.genCauchyMatrix(encodeMatrix, getNumAllUnits(), getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), getNumAllUnits());
        }
        gfTables = new byte[getNumAllUnits() * getNumDataUnits() * 32];
        RSUtil.initTables(getNumDataUnits(), getNumParityUnits(), encodeMatrix,
                getNumDataUnits() * getNumDataUnits(), gfTables);
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTables, -1));
        }
        preCompute();
    }

    @Override
    public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs)
            throws IOException {
        // [DEBUG] Inputs: data; Outputs: parity.
        ByteBufferEncodingState bbeState
                = new ByteBufferEncodingState(this, inputs, outputs, true);
        boolean usingDirectBuffer = bbeState.usingDirectBuffer;
        int dataLen = bbeState.encodeLength;
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
            doEncode(bbeState);
        } else {
            ByteArrayEncodingState baeState = bbeState.convertToByteArrayState();
            doEncode(baeState);
        }

        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i] != null) {
                // dataLen bytes consumed
                inputs[i].position(inputPositions[i] + dataLen);
            }
        }
    }


    @Override
    protected void doEncode(ByteBufferEncodingState encodingState) {
        throw new NotImplementedException("doEncode(ByteBufferEncodingState encodingState)");
        /*CoderUtil.resetOutputBuffers(
            encodingState.outputs,
            encodingState.encodeLength
        );
        RSUtil.encodeData(gfTables, encodingState.inputs, encodingState.outputs);*/
    }

    @Override
    protected void doEncode(ByteArrayEncodingState encodingState) {
        doEncode(encodingState, 1);
    }

    protected void doEncode(
        ByteArrayEncodingState encodingState,
        int erasedNodeIndex
    ) {
        CoderUtil.resetOutputBuffers(
            encodingState.outputs,
            encodingState.outputOffsets,
            encodingState.encodeLength
        );
        try {
            RSUtil.encodeData(
                    gfTables, encodingState.encodeLength,
                    encodingState.inputs, encodingState.inputOffsets,
                    encodingState.outputs, encodingState.outputOffsets
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        /*int sourceCount = encodingState.inputs.length +
            encodingState.outputs.length;
        byte[][] dataParityInputs
            = new byte[sourceCount][encodingState.encodeLength];
        combineDataParitySources(
            encodingState.inputs, encodingState.outputs,
            encodingState.inputOffsets, encodingState.outputOffsets,
            encodingState.encodeLength, dataParityInputs
        );

        int totalNodeCount = getNumAllUnits();
        // [WARN] We are assuming only one node is erased.
        int totalErasedCount = 1;
        for (int inputIndex = 0; inputIndex < totalNodeCount - totalErasedCount; inputIndex++) {
            // if (inputIndex > encodingState.outputs.length - 1) { continue; }
            // if (inputIndex == erasedNodeIndex) { continue; }
            int activeNodeIndex = erasedNodeIndex <= inputIndex ?
                inputIndex + 1 : inputIndex;
            repairTraceGeneration(activeNodeIndex, inputIndex, erasedNodeIndex,
                dataParityInputs, encodingState);
        }*/
    }

    protected void repairTraceGeneration(
        int nodeIndex, int inputIndex, int erasedNodeIndex,
        byte[][] inputs, ByteArrayEncodingState encodeState
    ) {
        assert(nodeIndex != erasedNodeIndex);
        byte bw = helperTable.getByte_9_6(nodeIndex, erasedNodeIndex, 0);
        byte[] repairTrace = new byte[bw * encodeState.encodeLength];
        byte[] H = helperTable.getRow_9_6(nodeIndex, erasedNodeIndex);
        byte[] Hij = new byte[H.length - 1];
        System.arraycopy(H, 1, Hij, 0, Hij.length);
        int idx = 0;
        for (int a = 0; a < bw; a++) {
            for (int testCodeWord = 0; testCodeWord < encodeState.encodeLength; testCodeWord++) {
                byte parityCalculation = (byte) (Hij[a] & (inputs[nodeIndex][testCodeWord]));
                int parityIndex = parityCalculation & 0xFF;
                repairTrace[idx++] = preComputedParity[parityIndex];
            }
        }

        int chunkLength = encodeState.encodeLength;
        int requiredWriteLength = bw * chunkLength;
        int totalWriteLength = chunkLength * 8;

        assert(requiredWriteLength <= totalWriteLength);
        encodeTrace(repairTrace, encodeState.outputs[inputIndex]);
    }

    public static void encodeTrace(byte[] source, byte[] output) {
        // [FIXME] Deal with output offsets.
        int bitToEncodeIndex = 0;
        for (byte bit : source) {
            assert(bit == 0 || bit == 1);
            int outputElementIndex = bitToEncodeIndex / 8;
            int bitPositionInElement = bitToEncodeIndex % 8;
            if (bit == 1) {
                output[outputElementIndex] |= (byte) (1 << (7 - bitPositionInElement));
            } else {
                output[outputElementIndex] &= (byte) ~(1 << (7 - bitPositionInElement));
            }
            bitToEncodeIndex++;
        }
    }

    private void combineDataParitySources(
        byte[][] dataInput, byte[][] parityInput,
        int[] dataInputOffsets, int[] parityInputOffsets,
        int encodeLength, byte[][] output
    ) {
        for (int i = 0; i < dataInput.length; i++) {
            int inputOffset = dataInputOffsets[i];
            System.arraycopy(dataInput[i], inputOffset, output[i], 0, encodeLength);
        }
        for (int i = 0; i < parityInput.length; i++) {
            int inputOffset = parityInputOffsets[i];
            System.arraycopy(parityInput[i], inputOffset, output[i + dataInput.length], 0, encodeLength);
        }
    }

    private void preCompute() {
        int i;
        preComputedParity[0] = 0;
        for (i = 1; i < 256; i++) {
            preComputedParity[i] = (byte) (preComputedParity[i >> 1] ^ (i & 1));
        }
    }
}
