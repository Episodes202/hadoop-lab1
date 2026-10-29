package lab;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** English term -> document frequencies and corpus total, using one MapReduce job. */
public final class InvertedIndex extends Configured implements Tool {
    private static final Pattern WORD = Pattern.compile("[A-Za-z]+");
    private static final String CACHE_NAME = "lab-stopwords.txt";

    public enum IndexCounters {
        RAW_TOKENS, STOPWORD_TOKENS, INDEXED_TOKENS, DISTINCT_TERMS, DOCUMENT_POSTINGS
    }

    /** Intermediate value preserves its type across zero, one, or many Combiner calls. */
    public static final class PostingWritable implements Writable {
        private String document = "";
        private long count;

        public PostingWritable() { }

        public PostingWritable(String document, long count) {
            this.document = document;
            this.count = count;
        }

        public String getDocument() { return document; }
        public long getCount() { return count; }

        @Override
        public void write(DataOutput output) throws IOException {
            Text.writeString(output, document);
            output.writeLong(count);
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            document = Text.readString(input);
            count = input.readLong();
        }
    }

    public static final class IndexMapper extends Mapper<LongWritable, Text, Text, PostingWritable> {
        private final Set<String> stopwords = new HashSet<>();
        private final Text term = new Text();
        private PostingWritable occurrence;

        @Override
        protected void setup(Context context) throws IOException {
            String document = ((FileSplit) context.getInputSplit()).getPath().getName();
            occurrence = new PostingWritable(document, 1L);
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(CACHE_NAME), StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.replace("\uFEFF", "").trim().toLowerCase(Locale.ROOT);
                    if (line.isEmpty() || line.startsWith("#")) continue;
                    for (String token : line.split("\\s+")) {
                        if (!token.matches("[a-z]+")) {
                            throw new IOException("Stopword must contain only ASCII letters: " + token);
                        }
                        stopwords.add(token);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            Matcher tokens = WORD.matcher(line.toString());
            while (tokens.find()) {
                String word = tokens.group().toLowerCase(Locale.ROOT);
                context.getCounter(IndexCounters.RAW_TOKENS).increment(1);
                if (stopwords.contains(word)) {
                    context.getCounter(IndexCounters.STOPWORD_TOKENS).increment(1);
                    continue;
                }
                term.set(word);
                context.write(term, occurrence);
                context.getCounter(IndexCounters.INDEXED_TOKENS).increment(1);
            }
        }
    }

    private static Map<String, Long> sumByDocument(Iterable<PostingWritable> values) throws IOException {
        Map<String, Long> counts = new TreeMap<>();
        for (PostingWritable value : values) {
            if (value.getCount() <= 0) throw new IOException("Nonpositive posting count");
            String document = value.getDocument();
            try {
                counts.put(document, Math.addExact(counts.getOrDefault(document, 0L), value.getCount()));
            } catch (ArithmeticException error) {
                throw new IOException("Document frequency exceeds signed 64-bit range", error);
            }
        }
        return counts;
    }

    public static final class CountCombiner extends Reducer<Text, PostingWritable, Text, PostingWritable> {
        @Override
        protected void reduce(Text term, Iterable<PostingWritable> values, Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<String, Long> entry : sumByDocument(values).entrySet()) {
                context.write(term, new PostingWritable(entry.getKey(), entry.getValue()));
            }
        }
    }

    public static final class IndexReducer extends Reducer<Text, PostingWritable, Text, Text> {
        @Override
        protected void reduce(Text term, Iterable<PostingWritable> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Long> counts = sumByDocument(values);
            StringBuilder postings = new StringBuilder();
            long total = 0;
            for (Map.Entry<String, Long> entry : counts.entrySet()) {
                postings.append('<').append(entry.getKey()).append(',').append(entry.getValue()).append(">;");
                try {
                    total = Math.addExact(total, entry.getValue());
                } catch (ArithmeticException error) {
                    throw new IOException("Corpus frequency exceeds signed 64-bit range", error);
                }
            }
            postings.append("<total,").append(total).append(">.");
            context.write(term, new Text(postings.toString()));
            context.getCounter(IndexCounters.DISTINCT_TERMS).increment(1);
            context.getCounter(IndexCounters.DOCUMENT_POSTINGS).increment(counts.size());
        }
    }

    /** Validate names because basenames are the output document identifiers. */
    private static void checkDocuments(FileSystem fs, Path path, Set<String> documents) throws IOException {
        FileStatus status = fs.getFileStatus(path);
        String name = path.getName();
        if (name.startsWith("_") || name.startsWith(".")) return;
        if (status.isDirectory()) {
            for (FileStatus child : fs.listStatus(path)) checkDocuments(fs, child.getPath(), documents);
        } else {
            if (name.equals("total") || name.matches(".*[,;<>\\s].*")) {
                throw new IOException("Document name cannot be 'total' or contain whitespace, comma, semicolon, <, >: " + name);
            }
            if (!documents.add(name)) throw new IOException("Duplicate document basename: " + name);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage: InvertedIndex INPUT OUTPUT STOPWORDS [REDUCERS]");
            return 2;
        }
        int reducers = args.length == 4 ? Integer.parseInt(args[3]) : 1;
        if (reducers <= 0) throw new IllegalArgumentException("REDUCERS must be positive");
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path stopwords = new Path(args[2]);
        FileSystem inputFs = input.getFileSystem(getConf());
        FileSystem outputFs = output.getFileSystem(getConf());
        FileSystem stopwordFs = stopwords.getFileSystem(getConf());
        if (outputFs.exists(output)) throw new IOException("Output already exists; choose a new path: " + output);
        if (!stopwordFs.isFile(stopwords)) throw new IOException("Stopword file does not exist: " + stopwords);
        Set<String> documents = new HashSet<>();
        checkDocuments(inputFs, input, documents);
        if (documents.isEmpty()) throw new IOException("No visible input documents found: " + input);

        Job job = Job.getInstance(getConf(), "experiment2-inverted-index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        if (getConf().getBoolean("lab.index.combiner.enabled", true)) job.setCombinerClass(CountCombiner.class);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PostingWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(reducers);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        URI uri = stopwordFs.makeQualified(stopwords).toUri();
        job.addCacheFile(new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), CACHE_NAME));
        System.out.println("INPUT_DOCUMENTS=" + documents.size());
        boolean success = job.waitForCompletion(true);
        System.out.println("JOB_ID=" + job.getJobID());
        if (success) {
            for (IndexCounters counter : IndexCounters.values()) {
                System.out.println(counter.name() + "=" + job.getCounters().findCounter(counter).getValue());
            }
        }
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InvertedIndex(), args));
    }
}
