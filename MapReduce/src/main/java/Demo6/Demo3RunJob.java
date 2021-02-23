package Demo6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * @author mars
 * 任务3 计算每一个url响应的平均时间，并按从大到小的顺序排列
 * 输出结果举例：
 * /service/1.htm 3.1
 * / service/3.htm 2.9
 * /html/notes/1.html 3.5
 * <p>
 * * String str = "012345";
 * * //[0, 2)
 * * String substring = str.substring(0, 2);
 * * System.out.println(substring);
 */
public class Demo3RunJob {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo6/demo6_3/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo6/demo6_3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Demo3");
        job.setJarByClass(Demo3RunJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Demo3Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(Demo3Reducer.class);
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

class Demo3Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        int i = line.indexOf("T") + 1;
        int j = line.indexOf("HTTP/1.0");
        String url = line.substring(i, j).trim();
        String str = new StringBuffer(line).reverse().toString();
        int k = str.indexOf(" ");
        String timeReverse = str.substring(0, k);
        String time = new StringBuffer(timeReverse).reverse().toString();
        context.write(new Text(url + "\t"), new DoubleWritable(Double.parseDouble(time)));
    }
}

class Demo3Reducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    TreeMap<Double, String> map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        for (DoubleWritable i : values) {
            sum += i.get();
            count++;
        }
        DecimalFormat df = new DecimalFormat("0.00");
        Double db = Double.valueOf(df.format(sum / count));
        map.put(db, key.toString());
        //context.write(key, new DoubleWritable(sum / count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList();
        for (Double i : map.keySet()) {
            list.add(map.get(i) + "\t" + i);
        }
        for (String j : list) {
            context.write(new Text(j), NullWritable.get());
        }

    }
}
