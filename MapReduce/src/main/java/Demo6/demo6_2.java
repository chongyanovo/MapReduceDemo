package Demo6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author mars
 */
public class demo6_2 {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo6/demo6_2/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo6/demo6_2/output");


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Map_demo6_2");
        job.setJarByClass(demo6_2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Map_demo6_2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Reduce_demo6_2.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleOutputs.addNamedOutput(job, "KeySpilt", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "AllPart", TextOutputFormat.class, NullWritable.class, Text.class);


        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Map_demo6_2 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int i = line.indexOf("[");
        int j = line.indexOf("]");
        String substring = line.substring(i, j);

        String[] time = substring.split(" ");
        context.write(new Text(time[0]), value);
    }
}

class Reduce_demo6_2 extends Reducer<Text, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text i : values) {
            mos.write("KeySpilt", NullWritable.get(), i, key.toString()+"/");
            mos .write("AllPart", NullWritable.get(), i);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (null != mos) {
            mos.close();
            mos = null;
        }
    }

}
