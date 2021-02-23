package Demo8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author mars
 * 任务2 评价表的“评级”字段内容不规范，有些是3级，有些是3，请统一按“级”修改，比如：3改为3级
 * 另外，评级字段有些是非法值，比如0级和6级，请删除对应的记录，并输出删除的数字+记录数量。
 * 要求：在任务1的基础上做任务2，并将新修改的评价表放到目录comment3
 */
public class Question2 {
    private static Path INPUT_PATH = new Path("hdfs://localhost:9000//Demo8/output/comment2");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Demo8/output/comment3");

//    private static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/out1/comment2");
//    private static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/comment3");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Question2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

class M2 extends Mapper<LongWritable, Text, Text, NullWritable> {
    int count = 0;
    ArrayList<String> list = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("计数器", "删除的非法值数量:");
        String[] line = value.toString().trim().split(",");
        String comment = line[2];
        if (comment.contains("级")) {
            comment = comment.replace("级", "");
        }
        int i = Integer.parseInt(comment);
        if (i > 0 && i < 6) {
            comment += "级";
            String str = line[0] + "," + line[1] + "," + comment + "," + line[3] + "," + line[4];
            list.add(str);
        } else {
            count++;
            counter.increment(1L);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //context.write(new Text("删除了" + count + "个非法值"), NullWritable.get());
        for (String i : list) {
            context.write(new Text(i), NullWritable.get());
        }
    }
}