package stratosphere;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by teots on 3/14/14.
 */
public class StratosphereTester implements Program, ProgramDescription {

    /**
     * Converts a Record containing one string in to multiple string/integer pairs.
     * The string is tokenized by whitespaces. For each token a new record is emitted,
     * where the token is the first field and an Integer(1) is the second field.
     */
    public static class TokenizeLine extends MapFunction implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void map(Record record, Collector<Record> collector) {
            // get the first field (as type StringValue) from the record
            String line = record.getField(0, StringValue.class).getValue();

            // normalize the line
            line = line.replaceAll("\\W+", " ").toLowerCase();

            // tokenize the line
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();

                // we emit a (word, 1) pair
                collector.collect(new Record(new StringValue(word), new IntValue(1)));
            }
        }
    }

    /**
     * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
     * in the record. The other fields are not modified.
     */
    @ReduceOperator.Combinable
    @FunctionAnnotation.ConstantFields(0)
    public static class CountWords extends ReduceFunction implements Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
            Record element = null;
            int sum = 0;
            while (records.hasNext()) {
                element = records.next();
                int cnt = element.getField(1, IntValue.class).getValue();
                sum += cnt;
            }

            element.setField(1, new IntValue(sum));
            out.collect(element);
        }

        @Override
        public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
            // the logic is the same as in the reduce function, so simply call the reduce method
            reduce(records, out);
        }
    }


    @Override
    public Plan getPlan(String... args) {
        // parse job parameters
        int numSubTasks = 4;
        String dataInput = "file:///home/teots/Desktop/text/";
        String output = "file:///home/teots/Desktop/out/";

        FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput, "Input Lines");
        MapOperator mapper = MapOperator.builder(new TokenizeLine())
                .input(source)
                .name("Tokenize Lines")
                .build();
        ReduceOperator reducer = ReduceOperator.builder(CountWords.class, StringValue.class, 0)
                .input(mapper)
                .name("Count Words")
                .build();
        FileDataSink out = new FileDataSink(new CsvOutputFormat(), output, reducer, "Word Counts");
        CsvOutputFormat.configureRecordFormat(out)
                .recordDelimiter('\n')
                .fieldDelimiter(' ')
                .field(StringValue.class, 0)
                .field(IntValue.class, 1);

        Plan plan = new Plan(out, "WordCount Example");
        plan.setDefaultParallelism(numSubTasks);

        return plan;
    }


    @Override
    public String getDescription() {
        return "Parameters: [numSubStasks] [input] [output]";
    }


    public static void main(String[] args) throws Exception {
        StratosphereTester wc = new StratosphereTester();

        Plan plan = wc.getPlan(args);

        // This will execute the word-count embedded in a local context. replace this line by the commented
        // succeeding line to send the job to a local installation or to a cluster for execution
        LocalExecutor.execute(plan);
//		PlanExecutor ex = new RemoteExecutor("localhost", 6123, "target/pact-examples-0.4-SNAPSHOT-WordCount.jar");
//		ex.executePlan(plan);
    }
}
