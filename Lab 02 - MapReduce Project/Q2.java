import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Q2 {

    public static class Q2Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable count = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse input
            String input = value.toString();
            String[] parts = input.split(";");
            String name = parts[0];
            String[] genders = parts[1].split(",\\ ?");
            String[] origins = parts[2].split(",\\ ?");
            // Map names to number of origins
            count.set(origins.length);
            context.write(count, one);
        }
    }

    public static class Q2Reducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "name count by number of origins");
        job.setJarByClass(Q2.class);
        job.setMapperClass(Q2Mapper.class);
        job.setCombinerClass(Q2Reducer.class);
        job.setReducerClass(Q2Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}