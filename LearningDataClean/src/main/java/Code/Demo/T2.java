package Demo;

import org.apache.hadoop.conf.Configuration;
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
    //2.计算人工智能学院平均成绩最高的是哪个课程？
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
//        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "T1");
        job.setJarByClass(T2.class);

        job.setInputFormatClass(TextInputFormat.class);
        //FileInputFormat.setInputPaths(job, new Path("H:\\测试\\in"));
        FileInputFormat.setInputPaths(job, new Path("/in"));

        job.setMapperClass(M2.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(R2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        //FileOutputFormat.setOutputPath(job, new Path("H:\\测试\\out2"));
        FileOutputFormat.setOutputPath(job, new Path("/out2"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M2 extends Mapper<LongWritable, Text, IntWritable, Text> {
    //0.分院 1.专业 2.班级 3.课程名称 4.班级人数 5.平均成绩
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(",");
        String course = arr[3];
        Integer AveScore = Integer.parseInt(arr[5]);
        context.write(new IntWritable(AveScore), new Text(course));
    }
}

class R2 extends Reducer<IntWritable, Text, Text, IntWritable> {
    TreeMap<IntWritable, Text> map = new TreeMap<IntWritable, Text>();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            map.put(key, i);
            if (map.size() > 1) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        IntWritable AveScore = map.firstKey();
        Text course =  map.get(AveScore);
        context.write(course, AveScore);
    }
}