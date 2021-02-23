package Code.wendusort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MyReducer extends Reducer<MyKey, Text, NullWritable,Text> {
    protected void redcue(MyKey k, Iterable<Text> v,Context ctx) throws IOException, InterruptedException {

        int sum = 0;
        for(Text t:v){
            sum++;
            if (sum>3){
                break;
            }
            else{
                ctx.write(NullWritable.get(),t);
            }
        }
    }
}
