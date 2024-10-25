package tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class BiggestAvgYearPerMassAndClassification extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path intermediate = new Path(args[1]);
        Path output = new Path(args[2]);

        Job job1 = Job.getInstance(conf);
        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(intermediate, true);
        FileOutputFormat.setOutputPath(job1, intermediate);

        job1.setJarByClass(BiggestAvgYearPerMassAndClassification.class);
        job1.setMapperClass(AvgMassMapper.class);
        job1.setReducerClass(AvgMassReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(AvgMassWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        if (job1.waitForCompletion(true)){
            Job job2 = Job.getInstance(conf);
            job2.setJarByClass(BiggestAvgYearPerMassAndClassification.class);

            FileInputFormat.addInputPath(job2, intermediate);
            FileOutputFormat.setOutputPath(job2, output);

            job2.setMapperClass(MaxAvgMapper.class);
            job2.setReducerClass(MaxAvgReducer.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(YearAvgWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // Executar o Job 2
            return job2.waitForCompletion(true) ? 0 : 1;
        }

        return 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new BiggestAvgYearPerMassAndClassification(), args);
        System.exit(result);

    }

    public static class AvgMassMapper extends Mapper<LongWritable, Text, Text, AvgMassWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            //Exemplo de string: Aachen,1,Valid,L5,21,Fell,1880,50.775000,6.083330,"(50.775, 6.08333)"


            if (columns.length > 6 && !line.startsWith("name")) {
                String recClass = columns[3]; //  RecClass
                String year = columns[6];     // Ano
                String mass = columns[4]; //massa

                try{
                    float massWeight = Float.parseFloat(mass);
                    // Emitir a chave como (RecClass_Ano) e o valor como a massa do meteorito.
                    context.write(new Text(recClass + "_" + year), new AvgMassWritable(massWeight, 1));
                } catch(NumberFormatException e) {
                    //ignora registro inválido
                }


            }
        }
    }

    public static class AvgMassReducer extends Reducer<Text, AvgMassWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<AvgMassWritable> values, Context context) throws IOException, InterruptedException {
            float sumMass = 0;
            int count = 0;

            for (AvgMassWritable val : values) {
                sumMass += val.getMass();
                count += val.getCount();
            }

            float avgMass = sumMass / count;

            context.write(key, new FloatWritable(avgMass));
        }
    }

    public static class MaxAvgMapper extends Mapper<LongWritable, Text, Text, YearAvgWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\\t");
            if (parts.length == 2) {
                String recClassYear = parts[0];
                String[] recClassYearSplit = recClassYear.split("_");
                if (recClassYearSplit.length == 2) {
                    String recClass = recClassYearSplit[0];
                    String year = recClassYearSplit[1];
                    float avgMass = Float.parseFloat(parts[1]);
                    context.write(new Text(recClass), new YearAvgWritable(year, avgMass));
                }
            }
        }
    }

    public static class MaxAvgReducer extends Reducer<Text, YearAvgWritable, Text, Text> {
        public void reduce(Text key, Iterable<YearAvgWritable> values, Context context) throws IOException, InterruptedException {
            String maxYear = "";
            float maxAvg = -100_000_000;
            for (YearAvgWritable val : values) {
                if (val.getAvgMass() > maxAvg) {
                    maxAvg = val.getAvgMass();
                    maxYear = val.getYear();
                }
            }
            context.write(key, new Text(maxYear + "\t" + maxAvg));
        }
    }




}
