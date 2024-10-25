package tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class MeteorAvgMassPerYear extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new MeteorAvgMassPerYear(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = Job.getInstance(conf, "mediaAno");
        job.setJarByClass(MeteorAvgMassPerYear.class);
        job.setMapperClass(MapForAverage.class);
        job.setReducerClass(ReduceForAverage.class);
        job.setCombinerClass(CombineForAverage.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MeteorWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, MeteorWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (columns.length > 6 && !line.startsWith("name")) {
                String year = columns[6];
                String massString = columns[4];

                if (massString != null && !massString.trim().isEmpty()) {
                    try {
                        float weight = Float.parseFloat(massString.trim());
                        MeteorWritable meteorWritable = new MeteorWritable(weight, 1);

                        context.write(new Text(year), meteorWritable);
                    } catch (NumberFormatException e) {

                        System.err.println("Erro ao converter peso: " + massString);
                    }
                } else {

                    System.err.println("Peso inválido ou ausente: " + massString);
                }
            }
        }
    }

    public static class CombineForAverage extends Reducer<Text, MeteorWritable, Text, MeteorWritable> {
        public void reduce(Text key, Iterable<MeteorWritable> values, Context context)
                throws IOException, InterruptedException {

            float sumWeight = 0f;
            int count = 0;

            for (MeteorWritable val : values) {
                sumWeight += val.getWeight();
                count += val.getCount();
            }

            MeteorWritable m = new MeteorWritable(sumWeight, count);
            context.write(key, m);
        }
    }

    public static class ReduceForAverage extends Reducer<Text, MeteorWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<MeteorWritable> values, Context context) throws IOException, InterruptedException {
            float sumWeight = 0f;
            int count = 0;

            for (MeteorWritable val : values) {
                sumWeight += val.getWeight();
                count += val.getCount();
            }

            float avgMass = (count == 0) ? 0 : sumWeight / count;

            context.write(key, new FloatWritable(avgMass));
        }
    }
}
