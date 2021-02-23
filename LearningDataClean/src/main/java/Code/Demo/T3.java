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
import java.util.HashSet;
import java.util.Set;

public class T3 {
    //3、计算人工智能学院有几个专业？每个专业有几个班？
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
//        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "T1");
        job.setJarByClass(T3.class);

        job.setInputFormatClass(TextInputFormat.class);
        //FileInputFormat.setInputPaths(job, new Path("H:\\测试\\in"));
        FileInputFormat.setInputPaths(job, new Path("/in"));

        job.setMapperClass(M3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(R3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        //FileOutputFormat.setOutputPath(job, new Path("H:\\测试\\out3"));
        FileOutputFormat.setOutputPath(job, new Path("/out3"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

//subject clazz
class M3 extends Mapper<LongWritable, Text, Text, Text> {
    //0.分院 1.专业 2.班级 3.课程名称 4.班级人数 5.平均成绩
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(",");
        String subject = arr[1];
        String clazz = arr[2];
        context.write(new Text(subject), new Text(clazz));
    }
}

class R3 extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<Text> set = new HashSet<Text>();
        Integer count = 0;
        for (Text i : values) {
            if (!set.contains(i)) {
                set.add(i);
                count++;
            }
        }
        context.write(key, new IntWritable(count));
    }
}
