package CommonFriends.Step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String str = "";
        for (Text i : values) {
            str += (i + "-");
        }
        str = str.substring(0, str.length() - 1);
        //str = str + "\t";
        context.write(new Text(str), key);
    }
}
