package ProjetoMapReduceHadoop.Questao5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class MeteorCountDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new MeteorCountDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = Job.getInstance(conf);
        job.setJobName("MeteorCount");
        job.setNumReduceTasks(1);


        FileInputFormat.addInputPath(job, input);

        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(MeteorCountDriver.class);
        job.setMapperClass(MeteorCountDriver.MeteorMapper.class);
        job.setReducerClass(MeteorCountDriver.MeteorReducer.class);

        job.setMapOutputKeyClass(YearClassification.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        return job.waitForCompletion(true) ? 0 : 1;

    }



    public static class MeteorMapper extends Mapper<Object, Text, YearClassification, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private YearClassification yearClassification = new YearClassification();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (columns.length > 6) {
                String year = columns[6];
                String classification = columns[3];
                yearClassification.setYear(year);
                yearClassification.setClassification(classification);
                context.write(new YearClassification(year, classification), one);
            }



        }
    }

    public static class MeteorReducer extends Reducer<YearClassification, IntWritable, Text, NullWritable> {

        private Text result = new Text();

        public void reduce(YearClassification key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }


            String output = "Year: " +  key.getYear() + " | " + "RecClass: " + key.getClassification() +  " | "  + "Count: " + sum;
            result.set(output);

            context.write(result, NullWritable.get());
        }
    }
}
