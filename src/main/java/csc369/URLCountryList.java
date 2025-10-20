package csc369;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlCountryList {
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
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length == 2) {
                String hostname = parts[0].trim();
                String country = parts[1].trim();
                context.write(new Text(hostname), new Text("CNTRY:" + country));}
        }
    }
    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String country = null;
            Set<String> urls = new HashSet<>();
            for (Text val : values){
                String s = val.toString();
                if (s.startsWith("CNTRY:")){
                    country = s.substring(6);}
                else if (s.startsWith("URL:")){
                    urls.add(s.substring(4));}
            }
            if (country != null){
                for (String url : urls){
                    context.write(new Text(url), new Text(country));}
            }
        }
    }
    public static class UrlReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text url, Iterable<Text> countries, Context context)
                throws IOException, InterruptedException {
            TreeSet<String> uniqueCountries = new TreeSet<>();
            for (Text c : countries){
                uniqueCountries.add(c.toString());}
            context.write(url, new Text(String.join(", ", uniqueCountries)));}
    }
    public static final Class<?> OUTPUT_KEY_CLASS = Text.class;
    public static final Class<?> OUTPUT_VALUE_CLASS = Text.class;}