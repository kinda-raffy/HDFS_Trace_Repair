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
package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

/**
 * Trace-Repair erasure decoder that decodes a block group.
 *
 * It implements {@link ErasureCoder}.
 */
@InterfaceAudience.Private
public class TRErasureDecoder extends ErasureDecoder {
    private RawErasureDecoder trRawDecoder;

    public TRErasureDecoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareDecodingStep(final ECBlockGroup blockGroup) {
        ECBlock[] inputBlocks = getTraceInputBlocks(blockGroup);
        ECBlock[] outputBlocks = getOutputBlocks(blockGroup);
        RawErasureDecoder rawDecoder = checkCreateTRRawDecoder();
        return new ErasureDecodingStep(
            inputBlocks,
            getErasedIndexes(getInputBlocks(blockGroup)),
            outputBlocks,
            rawDecoder
        );
    }
    protected ECBlock[] getTraceInputBlocks(ECBlockGroup blockGroup) {
        // [WARN] We assume that only a single node can fail for now.
        int erasedNodeCount = 1;
        ECBlock[] inputBlocks
                = new ECBlock[getNumDataUnits() + getNumParityUnits() - erasedNodeCount];
        System.arraycopy(
                blockGroup.getParityBlocks(),
                0, inputBlocks, 0,
                getNumDataUnits() + getNumParityUnits() - erasedNodeCount
        );
        return inputBlocks;
    }

    private RawErasureDecoder checkCreateTRRawDecoder() {
        if (trRawDecoder == null) {
            trRawDecoder = CodecUtil.createRawDecoder(getConf(),
                    ErasureCodeConstants.TR_CODEC_NAME, getOptions());
        }
        return trRawDecoder;
    }

    @Override
    public void release() {
        if (trRawDecoder != null) {
            trRawDecoder.release();
        }
    }
}
