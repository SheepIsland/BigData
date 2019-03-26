package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Violetta on 2019-03-22
 */
public class MultiRunner extends Configured implements Tool {

        public int run(String[] args) throws Exception {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Exception count");
            job.setJarByClass(MultiRunner.class);
            job.setMapperClass(ExceptionCounterMapper.class);
            job.setReducerClass(ExceptionCounterReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
            Job jobSort = Job.getInstance(conf, "Result");
            jobSort.setNumReduceTasks(1);
            jobSort.setJarByClass(MultiRunner.class);
            jobSort.setMapperClass(SortedMapper.class);
            jobSort.setReducerClass(SortedReducer.class);
            jobSort.setOutputKeyClass(Text.class);
            jobSort.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(jobSort, new Path(args[1]));
            FileOutputFormat.setOutputPath(jobSort, new Path(args[2]));

            return jobSort.waitForCompletion(true) ? 0 : 1;
        }
        public static void main(String[] args) throws Exception {
            ToolRunner.run(new MultiRunner(), args);
        }
}
