package ProjetoMapReduceHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MeteorsAggregationRecClassDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new MeteorsAggregationRecClassDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = Job.getInstance(conf, "MeteorsRecClassDriver");
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, input);

        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(MeteorsAggregationRecClassDriver.class);
        job.setMapperClass(AggregationMapper.class);
        job.setReducerClass(AggregationReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AggregationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text recClassKey = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("name")) {
                return;
            }
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (columns.length >= 4) {
                String recClass = columns[3].trim();
                recClassKey.set(recClass);
                context.write(recClassKey, one);
            }
        }
    }

    public static class AggregationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sumCount = 0;

            for (IntWritable value : values) {
                sumCount += value.get();
            }

            context.write(key, new IntWritable(sumCount));
        }
    }
}
