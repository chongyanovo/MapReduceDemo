package Test.Test2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

public class T2 {
    public static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test2/T2/input");
    public static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test2/T2/output");

    //    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test2/T2/input");
//    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test2/T2/output");
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T2");
        job.setJarByClass(T2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M2 extends Mapper<LongWritable, Text, Text, IntWritable> {
    int count = 0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        String[] line = value.toString().split(",");
//        String[] strs = {"分院", "专业", "班级", "课程名称", "班级人数", "平均成绩"};
        if ((str.indexOf("分院") == -1) && (str.indexOf("专业") == -1)
                && (str.indexOf("班级") == -1)&& (str.indexOf("课程名称") == -1)
                && (str.indexOf("班级人数") == -1)&& (str.indexOf("平均成绩") == -1)
        ) {
            context.write(new Text(line[3]), new IntWritable(Integer.parseInt(line[5])));
        }
//        if (strs.equals(line)) {
//
//        }else {
//            context.write(new Text(line[3]), new IntWritable(Integer.parseInt(line[5])));
//        }
//        if (count >= 1) {
//            context.write(new Text(line[3]), new IntWritable(Integer.parseInt(line[5])));
//        }
//        count++;
    }
}

class R2 extends Reducer<Text, IntWritable, Text, IntWritable> {
    TreeMap<Integer, String> map = new TreeMap<Integer, String>();
    int K = 1;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        int sum = 0;
        for (IntWritable i : values) {
            count++;
            sum += i.get();
        }
        map.put(sum / count, key.toString());
        if (map.size() > K) {
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Object i : map.keySet()) {
            context.write(new Text(map.get(i)), new IntWritable((int) i));
        }
    }
}
