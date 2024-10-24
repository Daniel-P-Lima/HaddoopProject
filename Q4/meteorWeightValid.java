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

public class meteorWeightValid extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        int numberReducers = Integer.parseInt(args[2]);

        Job job = Job.getInstance(conf);
        job.setJobName("MeteorWeightValid");
        job.setNumReduceTasks(numberReducers);

        job.setJarByClass(meteorWeightValid.class);
        job.setMapperClass(MeteorMapper.class);
        job.setReducerClass(MeteorReducer.class);
        job.setCombinerClass(MeteorCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(),new meteorWeightValid(), args);
        System.exit(result);
    }

    //LongWritable: posicao do byte na linha do arquivo
    //Text: recebe a linha
    //text: ano que sai
    //IntWritable: Indica ocorrencia do meteoro em ano X
    public static class MeteorMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Exemplo de string: Aachen,1,Valid,L5,21,Fell,1880,50.775000,6.083330,"(50.775, 6.08333)"
            //precisa chegar no peso em gramas INDICE 4
            String line = value.toString();
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (columns.length > 5 && !line.startsWith("name")){
                String validation = columns[2]; //validez do nome
                String mass = columns[4];

                // se for válido e a massa não tá vazia
                if (validation.equals("Valid") && !mass.isEmpty()){
                    float massConverted = Float.parseFloat(mass);
                    if (massConverted > 1000){
                        context.write(new Text("meteors with valid name and mass bigger than a kilo: "), new IntWritable(1));
                    }

                }
            }
        }
    }

    public static class MeteorCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v: values){
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class MeteorReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v: values){
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
