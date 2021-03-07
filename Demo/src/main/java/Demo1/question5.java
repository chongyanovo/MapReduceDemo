package Demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class question5 {
    private static Path INPATH = new Path("hdfs://localhost:9000/demo1/in");
    private static Path OUTPATH = new Path("hdfs://localhost:9000/demo1/out5");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(question5.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPATH);

        job.setMapperClass(mapper5.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//5、找到“燃料种类”为空的列，并将其值填写为“汽油”。
class mapper5 extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("计数器", "燃料种类为空");
        String[] lines = value.toString().trim().split(",");
        String fuel = lines[15];
        if (fuel.equals("") || fuel.equals(null) || fuel.equals("NULL") || fuel.equals("NAN")) {
            fuel = "汽油";
            counter.increment(1L);
        }
        String line = "";
        for (int i = 0; i < 14; i++) {
            line += lines[i] + "\t";
        }
        line += fuel + "\t";
        for (int i = 15; i < lines.length; i++) {
            if (i == lines.length - 1) {
                line += lines[i];
            } else {
                line += lines[i] + "\t";
            }
        }
        context.write(new Text(line), NullWritable.get());
    }
}