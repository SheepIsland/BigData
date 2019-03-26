package hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortedMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text exception = new Text();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String log = value.toString();
        String[] logs =  log.split("\\s+");
        IntWritable num = new IntWritable(Integer.parseInt(logs[1]));
        exception.set(logs[0]);
        context.write(exception, num);
    }

}
