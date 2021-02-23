package average;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 计算每个人的平均成绩
 */
public class Average {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        if (args.length != 2||args==null){
//            System.err.println("Please input right path!");
//            System.exit(0);
//        }
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        configuration.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(configuration, Average.class.getSimpleName());
        job.setJarByClass(Average.class);


        job.setMapperClass(AvgMap.class);
        //job.setCombinerClass(AvgReduce.class);
        job.setReducerClass(AvgReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path("H:\\average\\in"));
        FileOutputFormat.setOutputPath(job, new Path("H:\\average\\out"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class AvgMap extends Mapper<Object, Text, Text, IntWritable> {
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(" ");
        String name = line[0];
        int score = Integer.parseInt(line[1]);
        Counter counter = context.getCounter("hx-map", "average-1");
        counter.increment(1L);
        context.write(new Text(name), new IntWritable(score));
    }

    ;
}

class AvgReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        int avg = 0;
        Counter counter = context.getCounter("hx-reduce", "average-2");
        counter.increment(1L);
        Object x = counter.getValue();
        if (counter.getValue() >= 8) {
            System.out.println("hehe");
        }
        Iterator<IntWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            sum += iterator.next().get();
            count++;
        }
        avg = (int) sum / count;

        context.write(key, new IntWritable(avg));
    }
}