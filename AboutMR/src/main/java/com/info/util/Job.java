package com.info.util;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.log4j.Logger;

/**
 * @author panda 2018-12-17
 */
public class Job extends TaskJob {
    Logger log = Logger.getLogger(this.getClass());

    @SuppressWarnings("unchecked")
    public void run() throws InterruptedException, IOException {
        // 数据分片
        log.info("InputSplit start running.. ");
        Thread.sleep(2000);
        List<InputSplit> inputSplitList = inputFormat.getSplits(inpath);
        log.info("map()的数量为: " + inputSplitList.size());
        log.info("Mapper start running..");
        Thread.sleep(2000);
        // 清理map输出的临时目录
        deleteMapOutPutTempFile();
        // 开始Map任务
        int mapCount = 1;
        for (InputSplit inputSplit : inputSplitList) {
            mapper.mapperId = UUID.randomUUID().toString();
            Mapper.Context context = mapper.new Context();
            context.setInputSplit(inputSplit);
            log.info("任务ID mapperId[" + mapCount + "]:" + mapper.mapperId);
            mapCount++;
            mapper.run(context);
        }
        // 开始Reduce任务
        int reduceCount = 1;
        log.info("Reduce start running..");
        Thread.sleep(2000);
        if (reduce != null) {
            reduce.reduceId = UUID.randomUUID().toString();
            Reducer.Context context = reduce.new Context();
            Map<Object, List<Object>> mapOutputFile = getMapOutPutFile();
            log.info("归并排序.");
            Thread.sleep(2000);
            Map<Object, List<Object>> sortMap = new TreeMap<Object, List<Object>>(
                    new MapKeyComparator() {
                    });
            sortMap.putAll(mapOutputFile);
            List<Object> mapOutputList = new ArrayList<Object>();
            for (Object obj : sortMap.keySet())
                mapOutputList.add(obj);
            context.setDataMap(sortMap);
            context.setDataKey(mapOutputList);
            log.info("任务ID reduceId[" + reduceCount + "]:" + reduce.reduceId);
            Thread.sleep(2000);
            mapCount++;
            // 输出统计结果
            log.info("输出统计结果");
            Thread.sleep(2000);
            reduce.run(context);
        }
    }

    class MapKeyComparator implements Comparator<Object> {
        @Override
        public int compare(Object str1, Object str2) {
            return ((String) str1).compareTo((String) str2);
        }
    }
}