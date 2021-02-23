package com.info.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class InputFormat<K, V> {
    /**
     * 数据分片
     *
     * @param inpath
     * @return
     */
    public List<InputSplit> getSplits(String inpath) {
        File f = new File(inpath);
        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
        if (f.isFile()) {
            InputSplit inputSplit = new InputSplit();
            inputSplit.setList(GenFile.readFile(inpath));
            inputSplitList.add(inputSplit);
        } else if (f.isDirectory()) {
            File files[] = f.listFiles();
            for (File tempFile : files) {
                InputSplit inputSplit = new InputSplit();
                inputSplit.setList(GenFile.readFile(tempFile.getPath()));
                inputSplitList.add(inputSplit);
            }
        }
        return inputSplitList;
    }
}