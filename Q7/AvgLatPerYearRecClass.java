package tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class AvgLatPerYearRecClass extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        int numberReducers = Integer.parseInt(args[2]);

        Job job = Job.getInstance(conf);
        job.setJobName("AvgLatPerYearRecClass");
        job.setNumReduceTasks(numberReducers);

        job.setJarByClass(AvgLatPerYearRecClass.class);
        job.setMapperClass(AvgLatMapper.class);
        job.setReducerClass(AvgLatReducer.class);
        job.setCombinerClass(AvgLatCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgLatPerYearRecClassWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new AvgLatPerYearRecClass(), args);
        System.exit(result);
    }

    public static class AvgLatMapper extends Mapper<LongWritable, Text, Text, AvgLatPerYearRecClassWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //name,id,nametype,recclass,mass (g),fall,year,reclat,reclong,GeoLocation
            String line = value.toString();
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            //recClass 3, Ano 6, reclat 7

            if (columns.length > 7 && !line.startsWith("name")){
                String recClass = columns[3];
                String year = columns[6];
                float latitude = Float.parseFloat(columns[7]);
                String keyOut = year + "_" + recClass;
                context.write(new Text(keyOut), new AvgLatPerYearRecClassWritable(latitude, 1));
            }
        }
    }

    public static class AvgLatCombiner extends Reducer<Text, AvgLatPerYearRecClassWritable, Text, AvgLatPerYearRecClassWritable>{
        public void reduce(Text key, Iterable<AvgLatPerYearRecClassWritable> values, Context context) throws IOException, InterruptedException {
            float sumLat = 0;
            int sumCount = 0;

            for (AvgLatPerYearRecClassWritable val : values){
                sumLat += val.getLatitude();
                sumCount += val.getCount();
            }

            context.write(key, new AvgLatPerYearRecClassWritable(sumLat,sumCount));
        }
    }

    public static class AvgLatReducer extends Reducer<Text, AvgLatPerYearRecClassWritable, Text, Text>{
        public void reduce(Text key, Iterable<AvgLatPerYearRecClassWritable> values, Context context) throws IOException, InterruptedException {
            float sumLat = 0;
            int sumCount = 0;

            for (AvgLatPerYearRecClassWritable val : values){
                sumLat += val.getLatitude();
                sumCount += val.getCount();
            }

            float avgLat = sumCount == 0 ? 0 : sumLat / sumCount;

            AvgLatPerYearRecClassWritable result =  new AvgLatPerYearRecClassWritable(avgLat, sumCount);

            context.write(key, new Text(result.toString()));
        }
    }

}
