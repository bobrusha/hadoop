import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordMapper
        extends Mapper<Object, Text, Text, TextIntWritable> {
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        Pattern p = Pattern.compile("\\w+");
        Matcher m = p.matcher(value.toString());
        List<String> list = new LinkedList<>();
        while (m.find()) {
            list.add(m.group());
        }
        for (int i = 0; i < list.size() - 1; ++i) {
            context.write(new Text(list.get(i)), new TextIntWritable(list.get(i+1), 1));
        }
    }
}
