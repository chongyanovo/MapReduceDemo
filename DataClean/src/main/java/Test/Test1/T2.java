package Test.Test1;

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
import java.util.HashSet;
import java.util.Set;

public class T2 {
//    public static Path IN_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test1/T2/input");
//    public static Path OUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test1/T2/output");
    public static Path IN_PATH = new Path("hdfs://localhost:9000/MapReduce/Test1/T2/input");
    public static Path OUT_PATH = new Path("hdfs://localhost:9000/MapReduce/Test1/T2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T2");
        job.setJarByClass(T2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job,IN_PATH);

        job.setMapperClass(M2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(R2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = OUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUT_PATH)) {
            fileSystem.delete(OUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,OUT_PATH);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

class M2 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        context.write(new Text(line[0]), new Text(line[3]));
    }
}

class R2 extends Reducer<Text, Text, Text, IntWritable> {
    Set set = new HashSet<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Text i : values) {
            if (!set.contains(i)) {
                set.add(i);
                count++;
            }
        }
        context.write(key,new IntWritable(count));
    }
}
