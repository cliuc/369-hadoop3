package csc369;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class CountryUrlCount {
    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            String line = value.toString();
            if (line.trim().isEmpty()) return;

            String[] parts = line.split(" ");
            if (parts.length > 6){
                String hostname = parts[0];
                String url = parts[6];
                context.write(new Text(hostname), new Text("URL:" + url));}
        }
    }
    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            String line = value.toString().trim();
            if (line.isEmpty())
                return;
            String[] parts = line.split(",");
            if (parts.length == 2){
                String hostname = parts[0].trim();
                String country = parts[1].trim();
                context.write(new Text(hostname), new Text("CNTRY:" + country));}
        }
    }
    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            String country = null;
            Map<String, Integer> urlCount = new HashMap<>();
            for (Text val : values){
                String s = val.toString();
                if (s.startsWith("CNTRY:")){
                    country = s.substring(6);}
                else if (s.startsWith("URL:")){
                    String url = s.substring(4);
                    urlCount.put(url, urlCount.getOrDefault(url, 0) + 1);}
            }
            if (country != null){
                for (Map.Entry<String, Integer> entry : urlCount.entrySet()){
                    context.write(new Text(country + "\t" + entry.getKey()),
                            new IntWritable(entry.getValue()));}
            }
        }
    }
    public static final Class<?> OUTPUT_KEY_CLASS = Text.class;
    public static final Class<?> OUTPUT_VALUE_CLASS = IntWritable.class;}