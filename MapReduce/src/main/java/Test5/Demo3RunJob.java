package Test5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
 * 任务3、找出全校男生和女生数量差距最大的班级
 * 输出结果举例：
 * 分院名称  班级名称  男生数量  女生数量
 * 人工智能   大数据2001  25     5   【x=男生数量-女生数量=20，且x值为全校所有班级的最大值】
 * 数字商务   电子商务1901   5    30 【y=女生数量-男生数据=25，且y值为全校所有班级的最大值】
 */
public class Demo3RunJob {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo5/demo5_3/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo5/demo5_3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Demo3");
        job.setJarByClass(Demo3RunJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Demo3Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Demo3Reducer.class);
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

class Demo3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        int abs = Math.abs(Integer.parseInt(line[2]) - Integer.parseInt(line[3]));
        context.write(value, new IntWritable(abs));
    }
}

class Demo3Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    TreeMap<Integer, String> map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable i : values) {
            if (!map.containsValue(key)) {
                map.put(i.get(), key.toString());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList();
        for (Integer i : map.keySet()) {
            list.add(map.get(i));
        }
        Collections.reverse(list);
        String str = list.get(0).replace(",","\t");
        context.write(new Text("男生和女生数量差距最大的班级是:"), NullWritable.get());
        context.write(new Text(str), NullWritable.get());
    }
}

