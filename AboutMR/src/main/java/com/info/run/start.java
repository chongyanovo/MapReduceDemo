package com.info.run;

import java.io.File;

import org.apache.hadoop.io.Text;
import com.info.mapreduce.TestMap;
import com.info.mapreduce.TestReduce;
import com.info.util.InputFormat;
import com.info.util.Job;
/**
 * 模拟MapReduce运行(实现流程:)
 * 数据块->数据分片-> map()->map结果->分区->合并排序->合并后的数据-> reduce()->reduce结果->part文件
 * <p>
 * 为简化实验过程 特约定如下：
 * 1, map 输入输出类型：LongWritable, Text, Text, LongWritable
 * 2, reduce 输入输出类型：Text, LongWritable Text, Text
 * 3, 数据分片按文件个数分片
 * 4, reduce默认为一个1个
 */

/**
 * 样例数据：4063424,ITMC(珠澳)-N-清河东-M5934OLP备用-ER-QHD/WDM80×10G-H-OTM,3146451,4926-ITMC(珠澳)-N-清河东-OLP备用-ER-QHD/WDM80×10G-H-OTM,OptiX OSN 8800,0,7,929,MCA8,196612,4,5935-珠海通信中心方向-收,12,OCH,72,237,每信道信噪比最大值,24H,2018-11-30 15:00:00+08:00,24.70,dB,PMP_SNR_MAX,3,PML_NEAR_END_Tx,0,--,Over
 * 实验目的：统计第一个字段(复用段编号) 出现的次数
 */
public class start {
    public static void main(String[] args) {
        Job job = new Job();
        // 输入文件的目录或文件路径
        String inpath = "/Volumes/software/IdeaProjects/AboutMR/src/main/resources/data";
        // 项目样例数据
        String mapTempFile = new File("").getAbsolutePath();
        File file = new File(mapTempFile + "/simpleData");
        inpath = file.getPath();
        job.setMapper(new TestMap());
        job.setReduce(new TestReduce());
        job.setInputFormat(new InputFormat<Text, Text>());
        job.setInpath(inpath);
        job.submitJob();
    }
}