import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

public class SpectrumRecordReader extends RecordReader<Text, RawSpectrum> {

    // line example: 2012 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
    private static final Pattern LINE_PATTERN = Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");

    private final String[] names = new String[SpectrumInputFormat.NUMBER_OF_VARIABLES];
    private final Map<String, RawSpectrum> map = new TreeMap<>();
    private Iterator<String> mapIterator;

    private String currentDate;
    private RawSpectrum currentSpec;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        int deleteCount = 0;
        for (int i = 0; i < SpectrumInputFormat.NUMBER_OF_VARIABLES; ++i) {
            final Path path = combineFileSplit.getPath(i);
            final FileSystem fs = path.getFileSystem(context.getConfiguration());
            GZIPInputStream gzip = new GZIPInputStream(fs.open(path));
            LineReader reader = new LineReader(gzip, context.getConfiguration());
            Text text = new Text();
            while (reader.readLine(text) != 0) {
                String line = text.toString();
                Matcher matcher = LINE_PATTERN.matcher(line);
                if (matcher.matches()) {
                    StringTokenizer tokenizer = new StringTokenizer(line);

                    String date = "";
                    float[] vals = new float[tokenizer.countTokens() - 5];

                    //read date
                    for (int j = 0; j < 5; ++j) {
                        date += tokenizer.nextToken() + " ";
                    }
                    date = date.trim();

                    //read values
                    int j = 0;
                    while (tokenizer.hasMoreTokens()) {
                        vals[j] = Float.parseFloat(tokenizer.nextToken());
                        ++j;
                    }

                    //add to map
                    if (map.containsKey(date)) {
                        map.get(date).setField(path.getName(), vals);
                    } else {
                        RawSpectrum spectrum = new RawSpectrum();
                        spectrum.setField(path.getName(), vals);
                        map.put(date, spectrum);
                    }
                }
            }
            ++deleteCount;
        }

        System.out.println("Delete counter " + deleteCount);
        mapIterator = map.keySet().iterator();
        System.out.println("init");
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        System.out.println("nextKeyValue");

        while (mapIterator.hasNext()) {
            String date = mapIterator.next();
            RawSpectrum spectrum = map.get(date);
            if (spectrum.isValid()) {
                currentDate = date;
                currentSpec = spectrum;
                //System.out.println(date + " is valid");
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        // can be empty
    }

    @Override
    public Text getCurrentKey() {
        return new Text(currentDate);
    }

    @Override
    public RawSpectrum getCurrentValue() {
        return currentSpec;
    }

    @Override
    public float getProgress() {
        return 0.f;
    }

}
