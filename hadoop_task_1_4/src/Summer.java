import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

public class Summer
        extends Reducer<Text, TextIntWritable, Text, TextIntWritable> {
    public void reduce(Text key, Iterable<TextIntWritable> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> mapWithAll = new TreeMap<>();
        String mostCommonWord = null;
        Integer maxFrequency = 0;

        for (TextIntWritable textWithInt : values) {
            String text = textWithInt.getText();
            Integer frequency = textWithInt.getNumber();
            if (!mapWithAll.containsKey(text)) {
                mapWithAll.put(text, frequency);
                if (maxFrequency < frequency) {
                    mostCommonWord = text;
                    maxFrequency = frequency;
                }
            } else {
                Integer newFrequency = mapWithAll.get(text) + frequency;
                if (maxFrequency < newFrequency) {
                    mostCommonWord = text;
                    maxFrequency = newFrequency;
                }
                mapWithAll.remove(text);
                mapWithAll.put(text, newFrequency);
            }
        }
        if (mostCommonWord != null) {
            context.write(key, new TextIntWritable(mostCommonWord, maxFrequency));
        }
    }
}
