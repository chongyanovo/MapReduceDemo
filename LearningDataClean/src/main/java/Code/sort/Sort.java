package sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 对数据进行排序
 */
public class Sort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2 || args == null) {
            System.err.println("Please input Path!");
            System.exit(0);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, Sort.class.getSimpleName());
        job.setJarByClass(Sort.class);

        job.setMapperClass(NumberSortMap.class);
        job.setReducerClass(NumberSortReduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
        //请注意：必须去掉上面两行代码，因为Input和Output都不是文本，而是数字

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.waitForCompletion(true);
    }
}

class NumberSortMap extends Mapper<Object, Text, IntWritable, IntWritable> {
    private static IntWritable data = new IntWritable();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        data.set(Integer.parseInt(line));
        context.write(data, new IntWritable(1));
    }
}

class NumberSortReduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    //在数据达到reducer之前，mapreduce框架已经对这些数据按key排序了。
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            context.write(key, new Text(""));
        }
    }
}
