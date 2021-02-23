package Test.Test5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;

/*
任务2、统计所有分院的学生数量，按由高到低的顺序排序
输出结果举例：
人工智能   130
智能交通   95
数字商务 65
 */
public class Q2 {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test5/Q2/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test5/Q2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q2");
        job.setJarByClass(Q2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(MyMap2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReduce2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class MyMap2 extends Mapper<LongWritable, Text, Text, IntWritable> {
    HashMap<String, Integer> map = new HashMap<String, Integer>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        int sum = Integer.parseInt(line[2]) + Integer.parseInt(line[3]);
        if (map.containsKey(line[0])) {
            int tmp = map.get(line[0]);
            tmp += sum;
            map.put(line[0], tmp);
        } else {
            map.put(line[0], sum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String i : map.keySet()) {
            context.write(new Text(i), new IntWritable(map.get(i)));
        }
    }
}

class MyReduce2 extends Reducer<Text, IntWritable, Text, NullWritable> {
    TreeMap<Integer, String> map = new TreeMap();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable i : values) {
            map.put(i.get(), key.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayList<String> arry = new ArrayList();
        for (Integer i : map.keySet()) {
            String str = map.get(i) + "\t" + i;
            arry.add(str);
        }
        Collections.reverse(arry);
        for (String j : arry) {
            context.write(new Text(j), NullWritable.get());
        }
    }
}

