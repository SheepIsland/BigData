package hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class SortedReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private TreeMap<String, Integer> topLogs = new TreeMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val: values){
            sum += val.get();
        }
        topLogs.put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LinkedHashMap<String, Integer> reverseTopLogs = new LinkedHashMap<>();
        topLogs.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseTopLogs.put(x.getKey(), x.getValue()));


        int counter = 0;
        for (String key: reverseTopLogs.keySet()) {
            if (counter ++ == 5) break;
            result.set(reverseTopLogs.get(key));
            context.write(new Text(key),result);
        }
    }
}
