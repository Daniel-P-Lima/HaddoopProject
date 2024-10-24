package tde;


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

public class AvgMassPerFallYear extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        int numberReducers = Integer.parseInt(args[2]);

        Job job = Job.getInstance(conf);
        job.setJobName("AvgMassPerFallYear");
        job.setNumReduceTasks(numberReducers);

        job.setJarByClass(AvgMassPerFallYear.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setCombinerClass(AvgCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgMassPerFallYearWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new AvgMassPerFallYear(), args);
        System.exit(result);
    }

    public static class AvgMapper extends Mapper<LongWritable, Text, Text, AvgMassPerFallYearWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //name,id,nametype,recclass,mass (g),fall,year,reclat,reclong,GeoLocation
            String line = value.toString();
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (columns.length > 6 && !line.startsWith("name")){
                String year = columns[6];
                String mass = columns[4];
                String situation = columns[5];

                try {
                    float weightMass = Float.parseFloat(mass);
                    AvgMassPerFallYearWritable avg = new AvgMassPerFallYearWritable(weightMass, 1, year);
                    context.write(new Text("Situation: " +  situation + " | year:" + year), avg);
                } catch (NumberFormatException e) {
                    // Log e ignorar registros inválidos
                }
            }
        }
    }

    public static class AvgCombiner extends Reducer<Text, AvgMassPerFallYearWritable, Text, AvgMassPerFallYearWritable>{
        public void reduce(Text key, Iterable<AvgMassPerFallYearWritable> values, Context context) throws IOException, InterruptedException {
            float sumMass = 0;
            int sumCount = 0;
            String year = "";

            for (AvgMassPerFallYearWritable val : values){
                sumMass += val.getMass();
                sumCount += val.getCount();
                year = val.getYear();
            }
             context.write(key, new AvgMassPerFallYearWritable(sumMass, sumCount, year));

        }
    }

    public static class AvgReducer extends Reducer<Text, AvgMassPerFallYearWritable, Text, Text>{
        public void reduce(Text key, Iterable<AvgMassPerFallYearWritable> values, Context context) throws IOException, InterruptedException {
            float sumMass = 0;
            int sumCount = 0;
            String year = "";

            for (AvgMassPerFallYearWritable val : values){
                sumMass += val.getMass();
                sumCount += val.getCount();
                year = val.getYear();
            }

            float avg = sumMass/sumCount;

            context.write(key, new Text("| Avg Mass: " + avg));

        }

    }


}
