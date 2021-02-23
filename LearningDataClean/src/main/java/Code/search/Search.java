package Code.search;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *搜索引擎的用户搜索次数统计
 * 第二列为用户的uid
 * 20111230000001	aaaaaaaaa1	百度	1	1	www.baidu.com
 * 20111230000002	aaaaaaaaa2	腾讯	1	1	www.qq.com
 * 20111230000003	aaaaaaaaa3	百度	1	1	www.baidu.com
 */
public class Search
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        if (null == args || args.length != 2)
        {
            System.err.println("<Usage>: UidCollectot <input> <output>");
            System.exit(1);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, Search.class.getSimpleName());
        job.setJarByClass(Search.class);

        job.setMapperClass(UidMapper.class);
        job.setReducerClass(UidReducer.class);
        job.setMapperClass(UidMapper.class);
        job.setReducerClass(UidReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class UidMapper extends Mapper<Object, Text, Text, IntWritable>
{
    private Text uidText = new Text();

    protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException
    {
        String[] line = value.toString().split("\t");
        if (null != line && line.length == 6)
        {
            String uid = line[1];
            if (null != uid && !"".equals(uid.trim()))
            {
                uidText.set(uid);
                context.write(uidText, new IntWritable(1));
            }
        }
    }
}

class UidReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException, IOException {
        int sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }

        context.write(key, new IntWritable(sum));
    }
}