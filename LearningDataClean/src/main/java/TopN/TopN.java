package TopN;

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

/**
 * 计算排名最高的N个值 这里N取1
 * a	1
 * b	2
 * <p>
 * a	3
 * b	4
 */

public class TopN {
    public static String IN_PATH = "/TopN/in";
    public static String OUT_PATH = "/TopN/out";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "TopN");
        job.setJarByClass(TopN.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(IN_PATH));

        job.setMapperClass(M.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(R.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M extends Mapper<LongWritable, Text, IntWritable, Text> {
    public static int N = 1;
    TreeMap<Integer, String> map = new TreeMap<Integer, String>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(" ");
        String K = line[0];
        Integer V = Integer.parseInt(line[1]);
        map.put(V, K);
        if (map.size() > N) {
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer i : map.keySet()) {
            context.write(new IntWritable(i), new Text(map.get(i)));
        }
    }
}

class R extends Reducer<IntWritable, Text, Text, IntWritable> {
    public static int N = 1;
    TreeMap<Integer, String> map = new TreeMap<Integer, String>();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            map.put(key.get(), i.toString());
            if (map.size() > N) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer i : map.keySet()) {
            context.write(new Text(map.get(i)), new IntWritable(i));
        }
    }
}
