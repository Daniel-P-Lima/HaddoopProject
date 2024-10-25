package tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MeteorCountByTypeAndClass extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new MeteorCountByTypeAndClass(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.setFloat("massThreshold", 1000f);

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = Job.getInstance(conf, "Meteor Count by NameType and RecClass");
        job.setJarByClass(MeteorCountByTypeAndClass.class);

        job.setMapperClass(MeteorCountMapper.class);
        job.setCombinerClass(MeteorCountCombiner.class);
        job.setReducerClass(MeteorCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MeteorCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private float massThreshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            massThreshold = conf.getFloat("massThreshold", 1000f); // Exemplo de 1000g
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (columns[4].equals("mass (g)") || columns[4].isEmpty()) {
                return;
            }

            if (columns.length > 4) {
                String nameType = columns[2];
                String recClass = columns[3];

                if (!columns[4].isEmpty()) {
                    try {
                        float mass = Float.parseFloat(columns[4]);

                        if (mass > massThreshold) {
                            String keyOut = nameType + "_" + recClass;
                            context.write(new Text(keyOut), new IntWritable(1));
                        }
                    } catch (NumberFormatException e) {

                        System.err.println("Erro ao converter massa para número: " + columns[4]);
                    }
                }
            }
        }

    }

    public static class MeteorCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class MeteorCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
