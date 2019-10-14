import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import javax.xml.parsers.*;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;


// Hvor mange <nd> elementer det er i <way> xml noder som har en tag med highway=*

public class Four_20TopHighWayNodes {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        conf.set("startTag", "<way");
        conf.set("endTag", "</way>");

        Job job = Job.getInstance(conf, "xml count");

        job.setJarByClass(Four_20TopHighWayNodes.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setInputFormatClass(StartEndFileInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class StartEndFileInputFormat extends FileInputFormat < LongWritable, Text > {

        @Override
        public RecordReader < LongWritable, Text > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            StartEndRecordReader reader = new StartEndRecordReader();
            reader.initialize(split, context);

            return reader;
        }
    }

    public static class StartEndRecordReader extends RecordReader < LongWritable, Text > {

        private long start;
        private long pos;
        private long end;
        private FSDataInputStream fsin;
        private byte[] startTag;
        private byte[] endTag;
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private final DataOutputBuffer buffer = new DataOutputBuffer();


        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            FileSplit split = (FileSplit) genericSplit;


            Configuration job = context.getConfiguration();
            this.startTag = job.get("startTag").getBytes("utf-8");
            this.endTag = job.get("endTag").getBytes("utf-8");


            start = split.getStart();
            end = start + split.getLength();

            final Path file = split.getPath();
            FileSystem fs = file.getFileSystem(job);
            this.fsin = fs.open(split.getPath());
            fsin.seek(start);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return false;
                // save to buffer:
                if (withinBlock) buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException,
                InterruptedException {
            return value;
        }


        @Override
        public float getProgress() throws IOException,
                InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            }
        }


        @Override
        public void close() throws IOException {
            if (fsin != null) {
                fsin.close();
            }
        }
    }

    public static class TokenizerMapper extends Mapper < Object, Text, Text, IntWritable > {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            try {

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();

                Element aWay = document.getDocumentElement();

                // For å ta vare på id-en til highway-noder som skal printes ut
                String highwayId = "";

                int counter = 0;

                // Om denne way-noden har child-nodes henter jeg ut disse.
                if (aWay.hasChildNodes()) {

                    // Henter ut alle child-nodes til way-noden
                    NodeList childNodeList = aWay.getChildNodes();

                    // Går igjennom alle disse child-nodene
                    for (int j = 0; j < childNodeList.getLength(); j++) {

                        // Nåværende child-node i way
                        Node childNode = childNodeList.item(j);

                        // Teller antall nd-noder for denne way-noden
                        if (childNode.getNodeName().equals("nd")) {
                            counter++;
                        }

                        // Om way-en inneholder en tag som er highway- skal vi ta vare på denne noden
                        if (childNode.getNodeName().equals("tag")) {
                            if (childNode.getAttributes().getNamedItem("k").getTextContent().equals("highway")) {

                                // Nåværende way er en highway-way. Tar vare på dens ID
                                highwayId = aWay.getAttributes().getNamedItem("id").getTextContent();
                            }
                        }

                        context.write(new Text(highwayId), new IntWritable(counter));
                    }
                }

            } catch (SAXException exception) {
                // ignore
            } catch (ParserConfigurationException exception) {

            }
        }

    }

    public static class IntSumReducer
            extends Reducer < Text, IntWritable, Text, IntWritable > {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable < IntWritable > values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}




/*
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import javax.xml.parsers.*;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;


public class Four_20TopHighWayNodes {

    public static class TextIntPair implements WritableComparable<TextIntPair> {

        private Text first;
        private IntWritable second;

        public TextIntPair() {
            set(new Text(), new IntWritable());
        }

        public TextIntPair(String first, int second) {
            set(new Text(first), new IntWritable(second));
        }

        public TextIntPair(Text first, IntWritable second) {
            set(first, second);
        }

        public void set(Text first, IntWritable second) {
            this.first = first;
            this.second = second;
        }

        public Text getFirst() {
            return first;
        }

        public IntWritable getSecond() {
            return second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            first.write(out);
            second.write(out);
        }

        @Override
        public void readFields(DataInput in ) throws IOException {
            first.readFields(in);
            second.readFields(in);
        }

        @Override
        public int hashCode() {
            return first.hashCode() * 163 + second.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof TextIntPair) {
                TextIntPair tp = (TextIntPair) o;
                return first.equals(tp.first) && second.equals(tp.second);
            }
            return false;
        }

        @Override
        public String toString() {
            return first + "\t" + second;
        }

        @Override
        public int compareTo(TextIntPair tp) {
            int cmp = first.compareTo(tp.first);
            if (cmp != 0) {
                return cmp;
            }
            return second.compareTo(tp.second);
        }
    }

    public static class SecondPartitioner extends Partitioner<TextIntPair, NullWritable> {
        @Override
        public int getPartition(TextIntPair key, NullWritable value, int numPartitions) {
            // multiply by 127 to perform some mixing
            return Math.abs(key.getSecond().hashCode() * 127) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextIntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextIntPair ip1 = (TextIntPair) w1;
            TextIntPair ip2 = (TextIntPair) w2;

            int cmp = -ip1.getSecond().compareTo(ip2.getSecond()); //reverse

            if (cmp != 0) {
                return cmp;
            }

            return ip1.getFirst().compareTo(ip2.getFirst());
        }
    }

    public static class GroupComparator extends WritableComparator
    {
        protected GroupComparator() {
            super(TextIntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextIntPair ip1 = (TextIntPair) w1;
            TextIntPair ip2 = (TextIntPair) w2;
            return ip1.getSecond().compareTo(ip2.getSecond());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        conf.set("startTag", "<osm");
        conf.set("endTag", "</osm>");

        Job job = Job.getInstance(conf, "xml count");
        job.setJarByClass(Four_20TopHighWayNodes.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setPartitionerClass(SecondPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setReducerClass(IntSumReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);


        job.setMapOutputKeyClass(TextIntPair.class);
        job.setMapOutputValueClass(IntWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class StartEndFileInputFormat extends FileInputFormat < LongWritable, Text > {

        @Override
        public RecordReader < LongWritable, Text > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            StartEndRecordReader reader = new StartEndRecordReader();
            reader.initialize(split, context);

            return reader;
        }

    }

    public static class StartEndRecordReader extends RecordReader < LongWritable, Text > {

        private long start;
        private long pos;
        private long end;
        private FSDataInputStream fsin;
        private byte[] startTag;
        private byte[] endTag;
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private final DataOutputBuffer buffer = new DataOutputBuffer();


        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            FileSplit split = (FileSplit) genericSplit;


            Configuration job = context.getConfiguration();
            this.startTag = job.get("startTag").getBytes("utf-8");
            this.endTag = job.get("endTag").getBytes("utf-8");


            start = split.getStart();
            end = start + split.getLength();

            final Path file = split.getPath();
            FileSystem fs = file.getFileSystem(job);
            this.fsin = fs.open(split.getPath());
            fsin.seek(start);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return false;
                // save to buffer:
                if (withinBlock) buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException,
                InterruptedException {
            return value;
        }


        @Override
        public float getProgress() throws IOException,
                InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            }
        }


        @Override
        public void close() throws IOException {
            if (fsin != null) {
                fsin.close();
            }
        }
    }

    public static class TokenizerMapper extends Mapper < Object, Text, TextIntPair, IntWritable > {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            try {

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();
                Element root = document.getDocumentElement();

                // Henter way noder
                NodeList wayList = root.getElementsByTagName("way");

                HashMap highWayListe = new HashMap();

                // For å ta vare på id-en til highway-noder som skal printes ut
                String highwayId = "";

                // Tell opp alle ganger highway forekommer
                for(int i = 0; i < wayList.getLength(); i++) {

                    // Nåværende way
                    Node aWay = wayList.item(i);
                    // Teller for nd-noder
                    int counter = 0;

                    // Om denne way-noden har child-nodes henter jeg ut disse.
                    if(aWay.hasChildNodes()){

                        // Henter ut alle child-nodes til way-noden
                        NodeList childNodeList = aWay.getChildNodes();

                        // Går igjennom alle disse child-nodene
                        for(int j = 0; j < childNodeList.getLength(); j++) {

                            // Nåværende child-node i way
                            Node childNode = childNodeList.item(j);

                            // Teller antall nd-noder for denne way-noden
                            if(childNode.getNodeName().equals("nd")){
                                counter++;
                            }

                            // Om way-en inneholder en tag som er highway- skal vi ta vare på denne noden
                            if(childNode.getNodeName().equals("tag")){
                                if(childNode.getAttributes().getNamedItem("k").getTextContent().equals("highway")){

                                    // Nåværende way er en highway-way. Tar vare på dens ID
                                    highwayId = aWay.getAttributes().getNamedItem("id").getTextContent();
                                }
                            }
                        }

                        context.write(new TextIntPair(new Text(highwayId), new IntWritable(counter)), new IntWritable(counter));
                    }

                }

            } catch (SAXException exception) {
                // ignore
            } catch (ParserConfigurationException exception) {

            }
        }

    }

    public static class IntSumReducer
            extends Reducer < TextIntPair, IntWritable, Text, IntWritable > {
        private IntWritable result = new IntWritable();

        public void reduce(TextIntPair key, Iterable < IntWritable > values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key.getFirst(), result);
        }
    }
}
 */
