package com.info.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TaskJob {
    Mapper<?, ?, ?, ?> mapper;
    Reducer<?, ?, ?, ?> reduce;
    Reducer<?, ?, ?, ?> combiner;
    InputFormat<?, ?> inputFormat;
    String inpath;
    String outpath;

    public TaskJob() {
        mapper = null;
        reduce = null;
        combiner = null;
        inputFormat = null;
        inpath = "";
        outpath = "";
    }

    public InputFormat<?, ?> getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(InputFormat<?, ?> inputFormat) {
        this.inputFormat = inputFormat;
    }

    public Mapper<?, ?, ?, ?> getMapper() {
        return mapper;
    }

    public void setMapper(Mapper<?, ?, ?, ?> mapper) {
        this.mapper = mapper;
    }

    public Reducer<?, ?, ?, ?> getReduce() {
        return reduce;
    }

    public void setReduce(Reducer<?, ?, ?, ?> reduce) {
        this.reduce = reduce;
    }

    public Reducer<?, ?, ?, ?> getCombiner() {
        return combiner;
    }

    public void setCombiner(Reducer<?, ?, ?, ?> combiner) {
        this.combiner = combiner;
    }

    public String getInpath() {
        return inpath;
    }

    public void setInpath(String inpath) {
        this.inpath = inpath;
    }

    public String getOutpath() {
        return outpath;
    }

    public void setOutpath(String outpath) {
        this.outpath = outpath;
    }

    public void submitJob() {
        try {
            run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
    }

    /**
     * 清空map输出的临时目录的文件
     */
    public void deleteMapOutPutTempFile() {
        String mapTempFile = new File("").getAbsolutePath();
        File file = new File(mapTempFile + "/temp");
        for (File fileTemp : file.listFiles()) {
            fileTemp.delete();
        }
    }

    /**
     * read Map output data
     *
     * @return
     */
    public Map<Object, List<Object>> getMapOutPutFile() {
        List<String> list = new ArrayList<String>();
        String mapTempFile = new File("").getAbsolutePath();
        File file = new File(mapTempFile + "/temp");
        for (File fileTemp : file.listFiles()) {
            list.addAll(GenFile.readFile(fileTemp.getPath()));
        }
        Map<Object, List<Object>> resultMap = new HashMap<Object, List<Object>>();
        for (String k : list) {
            String str[] = k.split("#&@");
            if (str.length >= 2) {
                if (resultMap.containsKey(str[0])) {
                    resultMap.get(str[0]).add(str[1]);
                } else {
                    List<Object> listTemp = new ArrayList<Object>();
                    listTemp.add(str[1]);
                    resultMap.put(str[0], listTemp);
                }
            }
        }
        return resultMap;
    }
}