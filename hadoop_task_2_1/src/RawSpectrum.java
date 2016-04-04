import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;


public class RawSpectrum implements Writable {

    // file name example: 45005k2012.txt.gz
    public static final Pattern
            FILENAME_PATTERN = Pattern.compile("([0-9]{5})([dijkw])([0-9]{4})\\.txt\\.gz");

    private float[] i = null;
    private float[] j = null;
    private float[] k = null;
    private float[] w = null;
    private float[] d = null;

    public RawSpectrum() {
    }

    // sets one of the variables depending on the filename
    public void setField(String filename, float[] val) {
        Matcher matcher = FILENAME_PATTERN.matcher(filename);
        if (matcher.matches()) {
            String field = matcher.group(2);
            if (field.equals("i")) i = val;
            if (field.equals("j")) j = val;
            if (field.equals("k")) k = val;
            if (field.equals("w")) w = val;
            if (field.equals("d")) d = val;
        }
    }

    public int size() {
        int n = 0;
        if (i != null) ++n;
        if (j != null) ++n;
        if (k != null) ++n;
        if (w != null) ++n;
        if (d != null) ++n;
        return n;
    }

    public boolean isValid() {
        return size() == 5;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeFloatArray(out, i);
        writeFloatArray(out, j);
        writeFloatArray(out, k);
        writeFloatArray(out, w);
        writeFloatArray(out, d);
    }

    @Override
    public void readFields(DataInput in)
            throws IOException {
        i = readFloatArray(in);
        j = readFloatArray(in);
        k = readFloatArray(in);
        w = readFloatArray(in);
        d = readFloatArray(in);
    }

    private void writeFloatArray(DataOutput out, float[] xs)
            throws IOException {
        out.writeInt(xs.length);
        for (float x : xs) {
            out.writeFloat(x);
        }
    }

    private float[] readFloatArray(DataInput in)
            throws IOException {
        int size = in.readInt();
        float[] xs = new float[size];
        for (int i = 0; i < xs.length; ++i) {
            xs[i] = in.readFloat();
        }
        return xs;
    }

    @Override
    public String toString() {
        return "[i=" + Arrays.toString(i)
                + ",j=" + Arrays.toString(j)
                + ",k=" + Arrays.toString(k)
                + ",w=" + Arrays.toString(w)
                + ",d=" + Arrays.toString(d)
                + "]";
    }
}
