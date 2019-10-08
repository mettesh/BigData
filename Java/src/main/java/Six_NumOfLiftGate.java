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

// How many ways of types ”highway=path”, ”highway=service”, ”high- way=road”, ”highway=unclassified” contains a node with the tag ”barrier=lift gate”?

public class Six_NumOfLiftGate {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");

        conf.set("startTag", "<osm");
        conf.set("endTag", "</osm>");

        Job job = Job.getInstance(conf, "xml count");

        job.setJarByClass(Six_NumOfLiftGate.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setInputFormatClass(StartEndFileInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class StartEndFileInputFormat extends FileInputFormat <LongWritable, Text > {

        @Override
        public RecordReader< LongWritable, Text > createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

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

    public static class TokenizerMapper extends Mapper< Object, Text, Text, IntWritable > {

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable none = new IntWritable(0);

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            try {

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document document = builder.parse(is);
                document.getDocumentElement().normalize();
                Element root = document.getDocumentElement();


                // How many ways of types ”highway=path”, ”highway=service”, ”high- way=road”, ”highway=unclassified” contains a node with the tag ”barrier=lift gate”?


                // Henter way noder
                NodeList wayList = root.getElementsByTagName("way");

                boolean containsBarrier;
                int wayCounter;

                // Går igjennom alle way-taggene
                for (int i = 0; i < wayList.getLength(); i++) {

                    wayCounter = 0;
                    containsBarrier = false;

                    // Nåværende way
                    Node aWay = wayList.item(i);

                    // Om denne way-noden har child-nodes henter jeg ut disse.
                    if (aWay.hasChildNodes()) {

                        // Henter ut alle child-nodes til way-noden
                        NodeList childNodeList = aWay.getChildNodes();

                        // Går igjennom alle disse child-nodene
                        for (int j = 0; j < childNodeList.getLength(); j++) {

                            // Nåværende child-node i way
                            Node childNode = childNodeList.item(j);

                            //Om noden man kommer til er en Tag sjekker man denne
                            if (childNode.getNodeName().equals("tag")) {

                                //System.out.println("Noden man kommer til er en tag");

                                // Sjekker om way-en inneholder en node med ”barrier=lift gate”
                                if (childNode.getAttributes().getNamedItem("k").getTextContent().equals("barrier")) {

                                    //System.out.println("Noden inneholder attributten barrier");

                                    if (childNode.getAttributes().getNamedItem("v").getTextContent().equals("lift_gate")) {

                                        //System.out.println("Noden inneholder attributt barrier med verdi lift_gate");

                                        containsBarrier = true;
                                    }
                                }

                                // Sjekker om det er en highway av ønsket type
                                if (childNode.getAttributes().getNamedItem("k").getTextContent().equals("highway")) {

                                    // Om way-en er av riktig hghwaytype skal denne sjekkes

                                    if (isCorrectHighwayType(childNode)) {

                                        // Om denne wayen har vist seg å inneholde barrier skal det plusses på en
                                        if(containsBarrier) {

                                            // wayCounter++;

                                            context.write(new Text("Numbers of ways of type highway= " + childNode.getAttributes().removeNamedItem("v").getTextContent() + "  that a node with the tag ”bar-rier=liftgate”: " ), one);
                                        }
                                        else {
                                            context.write(new Text("Numbers of ways of type highway= " + childNode.getAttributes().removeNamedItem("v").getTextContent() + "  that a node with the tag ”bar-rier=liftgate”: " ), none);
                                        }
                                    }

                                }
                            }
                        }
                    }

                    // context.write(new Text("Antall way-er med riktig highway-type som inneholder taggen ”barrier=lift gate”"), new IntWritable(wayCounter));

                }


            } catch (SAXException exception) {
                System.out.println("SAXException: " + exception);
            } catch (ParserConfigurationException exception) {
                System.out.println("ParserConfigurationException: " + exception);
            }
        }

        private boolean isCorrectHighwayType(Node childNode) {
            //Sjekker om highway er en av disse typene: ”highway=path”, ”highway=service”, ”high- way=road”, ”highway=unclassified”

            return childNode.getAttributes().getNamedItem("v").getTextContent().equals("path")
                    || childNode.getAttributes().getNamedItem("v").getTextContent().equals("service")
                    || childNode.getAttributes().getNamedItem("v").getTextContent().equals("road")
                    || childNode.getAttributes().getNamedItem("v").getTextContent().equals("unclassified");
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