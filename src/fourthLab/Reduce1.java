package fourthLab;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce1 extends Reducer<Text, Text, Text, Text> {

	private static BufferedWriter bw;
	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("src/fourthLab/Reduce1Out.txt"));
//		Map<String, ArrayList<String>> reduceIn = MapReduceUtil.readReduce("src/fourthLab/Map1Out.txt");
//		
//		Reduce1 r = new Reduce1();
//		for (String key: reduceIn.keySet()) {
//			r.reduce(new Text(key), MapReduceUtil.stringListToTextList(reduceIn.get(key)), null);
//		}
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
		
		//Set up a document group for this minHash
		DocumentCluster dg = new DocumentCluster(key);
		
		//Add all the documents with the given minHash to the document group
		for (Text val: values) {
			dg.addDocument(val.toString());
		}
		
		if (context == null) {
			bw.write("" + "\t" + dg.toString());
			bw.newLine();
		} else {
			context.write(new Text(""), new Text(dg.toString()));
		}
	}
}
