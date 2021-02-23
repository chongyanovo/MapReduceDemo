package 练习;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text lines, Context context) throws IOException, InterruptedException {
        String[] line = lines.toString().split("\t");
        SortBean sortBean = new SortBean();
        sortBean.setWord(line[0]);
        sortBean.setNum(Integer.parseInt(line[1]));
        context.write(sortBean, NullWritable.get());
    }
}
