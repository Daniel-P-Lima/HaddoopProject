package ProjetoMapReduceHadoop.Questao9;

// Percentual de meteoros que caíram de uma determinada classificação.
// Job 1 - Chave (RecClass) -> Valor (Quantidade):
// Chave ("Total") -> Valor (QuantidadeTotal)
// Job 2 - Chave (RecClass) -> Valor (Percentual): Quantidade/quantidadetotal por classificação

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MeteorPercentDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new MeteorPercentDriver(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path outputJob1 = new Path(args[1]);

        Job job1 = Job.getInstance(conf, "Job1 - Contagem de Meteoros por Classificação");
        job1.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(outputJob1, true);
        FileOutputFormat.setOutputPath(job1, outputJob1);

        job1.setJarByClass(MeteorPercentDriver.class);
        job1.setMapperClass(RecClassMapper.class);
        job1.setReducerClass(RecClassReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        boolean job1Success = job1.waitForCompletion(true);
        if (!job1Success) {
            return 1;
        }


        int totalMeteors = getTotalMeteors(outputJob1, conf);


        Path outputJob2 = new Path(args[2]);
        conf.setInt("total.meteors", totalMeteors);

        Job job2 = Job.getInstance(conf, "Job2 - Cálculo do Percentual de Meteoros por Classificação");
        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, outputJob1);
        FileSystem.get(conf).delete(outputJob2, true);
        FileOutputFormat.setOutputPath(job2, outputJob2);

        job2.setJarByClass(MeteorPercentDriver.class);
        job2.setMapperClass(PercentMapper.class);
        job2.setReducerClass(PercentReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    private int getTotalMeteors(Path outputPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path totalFile = new Path(outputPath, "part-r-00000");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(totalFile)))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("Total:")) {
                    String[] parts = line.split("\t");
                    return Integer.parseInt(parts[1].trim());
                }
            }
        }
        return 0;
    }



    public static class RecClassMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("name")) {
                return;
            }
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (columns.length >= 3 && !columns[3].isEmpty()) {
                String recClass = columns[3].trim();
                context.write(new Text(recClass), one);
            }
        }
    }


    public static class RecClassReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int totalCount = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sumCount = 0;

            for (IntWritable value : values) {
                sumCount += value.get();
            }


            totalCount += sumCount;

            context.write(key, new IntWritable(sumCount));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            context.write(new Text("Total:"), new IntWritable(totalCount));
        }
    }



    public static class PercentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2 && !parts[0].equals("Total:")) {
                String recClass = parts[0].trim();
                int count = Integer.parseInt(parts[1].trim());
                context.write(new Text(recClass), new IntWritable(count));
            }
        }
    }

    public static class PercentReducer extends Reducer<Text, IntWritable, Text, Text> {
        private int totalMeteors;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalMeteors = context.getConfiguration().getInt("total.meteors", 0);
            if (totalMeteors == 0) {
                throw new IOException("O total de meteoros é zero! Verifique o Job 1.");
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }

            if (totalMeteors > 0) {
                double percent = (count / (double) totalMeteors) * 100;
                context.write(key, new Text(String.format("%.2f%%", percent)));
            } else {
                context.write(key, new Text("0.00%"));
            }
        }
    }
}

