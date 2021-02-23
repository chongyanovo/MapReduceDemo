package CommonFriends.Step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        String str = "";
        for (Text i : values) {
            str += (i + "-");
            count++;
        }
        str = str.substring(0, str.length() - 1);
        context.write(key, new Text("有"+count +"个共同好友，分别是"+ "\t" + str));
    }
}
