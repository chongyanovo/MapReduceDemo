package Test6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

/**
 * @author mars
 * 任务3 计算每一个url响应的平均时间，并按从大到小的顺序排列
 * 输出结果举例：
 * /service/1.htm 3.1e
 * / service/3.htm 2.9
 * /html/notes/1.html 3.5
 */
public class Q3 {
    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test/input");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test/output/output3");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Q3.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(R3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M3 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String post = "POST";
        String get = "GET";
        String http = "HTTP/1.0";
        Key1 key1 = new Key1();
        int i;
        int j = line.indexOf(http);
        if (line.contains(post)) {
            i = line.lastIndexOf(post);
        } else {
            i = line.lastIndexOf(get);
        }
        String url = line.substring(i, j).trim();
        int k = line.lastIndexOf(" ");
        Double time = Double.valueOf(line.substring(k));
        context.write(new Text(url), new DoubleWritable(time));
    }
}

class R3 extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    TreeMap<Double, String> map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        for (DoubleWritable i : values) {
            sum += i.get();
            count++;
        }
        map.put(sum / count, key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList();
        for (Double i : map.keySet()) {
            list.add(map.get(i) + "\t" + i);
        }
        Collections.reverse(list);
        for (String j : list) {
            context.write(new Text(j), NullWritable.get());
        }
    }
}