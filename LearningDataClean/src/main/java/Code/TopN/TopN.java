package Code.TopN;

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
 * 计算排名最高的N个值
 */
public class TopN {
    public static String INPUT_PATH = "H:\\MapReducer\\TopN\\in";
    public static String OUT_PATH = "H:\\MapReducer\\TopN\\out";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows

        Job job = Job.getInstance(conf, TopN.class.getSimpleName());
        job.setJarByClass(TopN.class);
        job.setMapperClass(KMap.class);
        //必须把下面这行注释掉，不然会报错：Error: java.io.IOException: wrong key class: class org.apache.hadoop.io.Text is not class org.apache.hadoop.io.IntWritable
//      job.setCombinerClass(KReduce.class);
        job.setReducerClass(KReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
        System.out.println(job.waitForCompletion(true));
    }
}

class KMap extends Mapper<LongWritable, Text, IntWritable, Text> {

    TreeMap<Integer, String> map = new TreeMap<Integer, String>();
    public static final int K = 2;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.trim().length() > 0 && line.indexOf("\t") != -1) {
            String[] arr = line.split("\t", 2);
            String name = arr[0];
            Integer num = Integer.parseInt(arr[1]);
            map.put(num, name);
            if (map.size() > K) {//map中只保留num最高的K个值
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for (Integer num : map.keySet()) {
            context.write(new IntWritable(num), new Text(map.get(num)));
        }
    }
}

class KReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
    TreeMap<Integer, String> map = new TreeMap<Integer, String>();
    public static final int K = 2;

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        map.put(key.get(), values.iterator().next().toString());
        if (map.size() > K) {
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        for (Integer num : map.keySet()) {
//                context.write(new IntWritable(num), new Text(map.get(num)));
            context.write(new Text(map.get(num)), new IntWritable(num));
        }
    }
}