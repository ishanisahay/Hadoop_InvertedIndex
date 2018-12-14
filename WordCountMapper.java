import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {


    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String regex = "[^a-zA-Z]";
    //private StringBuilder finalWord = new StringBuilder();

    public void map(LongWritable key, Text value,
                    Context context) throws IOException, InterruptedException {
        String line = value.toString().replaceAll("(\\p{Punct}|\\d)+", " ").replaceAll("\t", " ").toLowerCase();

        //String newLine = line.replaceAll("(\\p{Punct})+", " ");
        char[] newLine = line.toCharArray();
        char[] temp = new char[newLine.length];
        int j = 0;
        //String finalWord = new String();
        for (int i = 0 ; i < newLine.length; i++)
        {
            if (Character.isLetter(newLine[i]) || newLine[i] == ' ')
            {
                //finalWord = finalWord + newLine[i];
                temp[j] = newLine[i];
                j = j+1;
            }

        }
        String finalWord = new String(temp);
        //line.replaceAll("\\p{Punct}|\\d", "").toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(finalWord);
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            //String newWord = word.toString().toLowerCase();
            context.write(word, new Text(fileName));
          
        }
    }
}
