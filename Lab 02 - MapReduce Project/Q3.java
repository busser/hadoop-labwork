import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Q3 {

    public enum inputCounter {n}

    public static class Q3Mapper extends Mapper<Object, Text, Text, FloatWritable> {

        private final static FloatWritable one = new FloatWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse input
            String input = value.toString();
            String[] parts = input.split(";");
            String name = parts[0];
            String[] genders = parts[1].split(",\\ ?");
            String[] origins = parts[2].split(",\\ ?");
            // Map names to genders
            for (String gender : genders) {
                context.write(new Text(gender), one);
            }
            // Increment input counter
            context.getCounter(Q3.inputCounter.n).increment(1);
        }
    }

    public static class Q3Combiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }

    }

    public static class Q3Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }

            // Get amount of names
            Cluster cluster = new Cluster(context.getConfiguration());
            long nameCount = cluster.getJob(context.getJobID()).getCounters().findCounter(inputCounter.n).getValue();

            result.set(sum / nameCount * 100.0);
            context.write(key, result);

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "gender proportion");
        job.setJarByClass(Q3.class);
        job.setMapperClass(Q3Mapper.class);
        job.setCombinerClass(Q3Combiner.class);
        job.setReducerClass(Q3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}