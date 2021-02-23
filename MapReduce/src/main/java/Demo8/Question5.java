package Demo8;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;


/**
 * @author mars
 * 任务5 对总表的数据进行分析，按销售量从高到低的顺序对出版社进行排序:
 * 出版社名称      销售总量
 * 机械工业出版社    57
 * 电子工业出版社    49
 * 南京大学出版社    28
 * ……
 */
public class Question5 {
    final static Path INPUT_PATH = new Path("hdfs://localhost:9000/Demo8/total/step2");
    final static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Demo8/out5");

//    private static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/total/step2");
//    private static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/out5");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Question32.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M5.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R5.class);
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

class M5 extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        //5,6
        context.write(new Text(line[5]), new IntWritable(Integer.parseInt(line[6])));
    }
}

class R5 extends Reducer<Text, IntWritable, Text, NullWritable> {
    TreeMap<Integer, String> map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        map.put(sum, key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList<>();
        for (Integer i : map.keySet()) {

            list.add(map.get(i) + "\t" + i);
        }
        Collections.reverse(list);
        for (String j : list) {
            context.write(new Text(j), NullWritable.get());
        }
    }
}