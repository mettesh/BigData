import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class One_BuildingCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    //Metode vi overrider fra Mapper
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      // Må gjøre om text-verdien om til String (Da vi ikke kan jobbe med text-datatypen)
      // Vi må deretter dele denne linjen med text opp i ord ved å benytte StringTokenizer
      // Kan her sette hva vi skal dele på:
      StringTokenizer itr = new StringTokenizer(value.toString());
      // Går nå igjennom alle ordene:
      while (itr.hasMoreTokens()) {

        // Legger et ord i aWord og gjør det til lowerCase
        String aWord = itr.nextToken();

        //Om et av ordene er tomme ønsker jeg å gå videre (Vil ikke ha denne som nøkkel)
        if(!(aWord.equals("k=\"building\""))){
          continue;
        }

        // Setter dette ordet til å være text (Da hadoop ikke leser String, men text)
        word.set(aWord);

    //OPPRETTER KEY,VALUE:
        // Setter nå en ordet til å være key, og deretter en value
        // Disse må være på datatype som hadoop forstår (one = intWriteble 1)
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;

      // Går igjennom alle verdiene i values til denne nøkkelen
      for (IntWritable val : values) {
        // Legger til denne verdien i sum
        sum += val.get();
      }
      result.set(sum);

      // Setter nå sammen nøkkelen med alle verdiene til denne (summen som ble lagt sammen over)
      context.write(key, result);

    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2){
      System.err.println("Invalid arguments!\n");
    }

    Configuration conf = new Configuration();
    conf.addResource("hdfs-site.xml");

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(One_BuildingCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
