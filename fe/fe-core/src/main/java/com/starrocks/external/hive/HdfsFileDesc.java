// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.starrocks.external.hive.text.TextFileFormatDesc;

public class HdfsFileDesc {
    private String fileName;
    private String compression;
    private long length;
    private ImmutableList<HdfsFileBlockDesc> blockDescs;
    private boolean splittable;
    private TextFileFormatDesc textFileFormatDesc;
    private ImmutableList<String> hudiDeltaLogs;
    private boolean isHudiMORTable;

    public HdfsFileDesc(String fileName, String compression, long length,
                        ImmutableList<HdfsFileBlockDesc> blockDescs, ImmutableList<String> hudiDeltaLogs,
                        boolean splittable, TextFileFormatDesc textFileFormatDesc, boolean isHudiMORTable) {
        this.fileName = fileName;
        this.compression = compression;
        this.length = length;
        this.blockDescs = blockDescs;
        this.hudiDeltaLogs = hudiDeltaLogs;
        this.splittable = splittable;
        this.textFileFormatDesc = textFileFormatDesc;
        this.isHudiMORTable = isHudiMORTable;
    }

    public String getFileName() {
        return fileName;
    }

    public String getCompression() {
        return compression;
    }

    public long getLength() {
        return length;
    }

    public ImmutableList<HdfsFileBlockDesc> getBlockDescs() {
        return blockDescs;
    }

    public boolean isSplittable() {
        return splittable;
    }

    public TextFileFormatDesc getTextFileFormatDesc() {
        return textFileFormatDesc;
    }

    public ImmutableList<String> getHudiDeltaLogs() {
        return hudiDeltaLogs;
    }

    public boolean isHudiMORTable() {
        return isHudiMORTable;
    }

}
