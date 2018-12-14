import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        int samefile = 0;
        String fileName = "";
        String textToWrite = "";
        String fileNameWrite = "";
        int pos= 0;
        for (Text value: values)
        {
            if(samefile == 0){
                fileName = value.toString();
                samefile = 1;
                pos =  fileName.lastIndexOf(".");
                fileNameWrite = fileName.substring(0,pos);

            }
            if(fileName.equals(value.toString())){
                sum = sum+1;
            }
            else {
                textToWrite += fileNameWrite + ":" + sum + " ";
                fileName = value.toString();
                pos =  fileName.lastIndexOf(".");
                fileNameWrite = fileName.substring(0,pos);
                sum = 1;
            }
        }
        textToWrite += fileNameWrite + ":" + sum + "\n";
        context.write(key, new Text(textToWrite));

    }
}
