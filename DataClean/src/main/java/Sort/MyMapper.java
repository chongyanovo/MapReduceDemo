package Sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String word = line[0];
        int num = Integer.parseInt(line[1]);
        SortBean sortBean = new SortBean();
//        sortBean.setWord(word);
//        sortBean.setNum(num);
//        context.write(sortBean, NullWritable.get());
    }
}
