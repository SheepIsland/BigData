package hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ExceptionCounterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text exception = new Text();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String log = value.toString();

        if (log.length() == 0) return;

        if (log.contains("/ERROR]:")&&(log.startsWith("[2017-"))){
            if (!exception.toString().contains("/ERROR]:") && exception.toString().contains("at")){
                int index = exception.find("(");
                String resultLog = exception.toString().substring(4,index);
                exception.set(resultLog);
                context.write(exception, one);
                exception.set("");
            } else exception.set(log);
        } else if (log.startsWith("[2017-")) return;

        if (log.contains("...") && log.contains("more")){
            int index = exception.find("(");
            String resultLog = exception.toString().substring(4,index);
            exception.set(resultLog);
            context.write(exception, one);
            exception.set("");
        } else exception.set(log);
    }

}
