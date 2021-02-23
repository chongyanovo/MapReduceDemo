package 考试;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 区域	采集时间	           气温	采集地点
 * 仙林	2019.12.16 14:00	12	学则路
 * 仙林	2019.12.13 15:00	14	亚东城
 * 鼓楼	2019.12.16 14:00	13	西康路
 * 鼓楼	2019.12.13 17:00	20	西康路
 * 江宁	2019.12.14 10:00	10	九龙湖
 * 江宁	2019.12.10 12:00	12	将军山
 * 江宁	2019.12.10 14:00	13	将军山
 *
 * 找出南京气温最高的采集点和对应的采集时间
 */
public class QiXiang {
    public static String INPUT_PATH = "D:\\hadoop-local-test\\input";
    public static String OUT_PATH = "D:\\hadoop-local-test\\output";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows

        Job job = Job.getInstance(conf, "qixiang");
        job.setJarByClass(QiXiang.class);
        job.setMapperClass(QiXiangMap.class);
        job.setReducerClass(QiXiangReduce.class);

        //设置map的k2 v2类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(QiXiangBean.class);

        //设置reduce的k2 v2类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
        System.out.println(job.waitForCompletion(true));
    }
}

class QiXiangMap extends Mapper<LongWritable, Text, IntWritable, QiXiangBean> {

    TreeMap<Integer, QiXiangBean> map = new TreeMap<Integer, QiXiangBean>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] arr = line.split(" ");
        Integer qiwen = Integer.parseInt(arr[3]);
        String time = arr[1] +" " + arr[2];
        String location = arr[4];

        QiXiangBean bean  = new QiXiangBean();
        bean.setLocation(location);
        bean.setTime(time);

        map.put(qiwen, bean);
        if (map.size() > 1) {//map中只保留num最高的K个值
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for (Integer num : map.keySet()) {
            context.write(new IntWritable(num), map.get(num));
        }
    }
}

class QiXiangReduce extends Reducer<IntWritable, QiXiangBean, Text, Text> {
    TreeMap<Integer, QiXiangBean> map = new TreeMap<Integer, QiXiangBean>();

    @Override
    public void reduce(IntWritable key, Iterable<QiXiangBean> values, Context context) throws IOException, InterruptedException {
        //一个最高温度可能对于多个采集地点，这里做简化处理，只保留一个采集节点。
        for (QiXiangBean bean : values)        {
            map.put(key.get(), bean);
            if (map.size() > 1) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        Integer key = map.firstKey();
        QiXiangBean bean = map.get(key);
        context.write(new Text(bean.getLocation()),new Text(bean.getTime()));
    }
}