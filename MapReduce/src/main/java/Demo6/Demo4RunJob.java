package Demo6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author mars
 * 任务4、将每一个访问ip访问的url放到不同的目录下（目录名称为ip）
 * 输出结果举例：
 * 目录名称：172.18.35.1
 * 数据内容：
 * 172.18.35.1 - - [2013-03-04 23:38:27] "POST /service/1.htm HTTP/1.0" 200 1
 * 172.18.35.1 - - [2013-03-05 23:38:27] "POST / service/12.htm HTTP/1.0" 200  2
 * 172.18.35.1 - - [2013-03-09 23:38:27] "POST html/notes/1.html HTTP/1.0" 200 2
 * 172.18.35.1 - - [2013-04-04 23:38:27] "GET html/notes/2.html HTTP/1.0" 200 1
 */
public class Demo4RunJob {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo6/demo6_4/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo6/demo6_4/output");


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Demo4");
        job.setJarByClass(Demo4RunJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Demo4Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Demo4Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //MultipleOutputs.addNamedOutput(job, "KeySpilt", TextOutputFormat.class, NullWritable.class, Text.class);


        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

class Demo4Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int i = line.indexOf("-");
        String ip = line.substring(0, i).trim();
        context.write(new Text(ip), value);
    }
}

class Demo4Reducer extends Reducer<Text, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            //mos.write("KeySpilt", NullWritable.get(), i.toString(),key+"/");
            mos.write(i, NullWritable.get(), key + "/");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        mos = null;
    }
}
