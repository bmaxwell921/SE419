package thirdLab.exp1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce3 extends Reducer<IntWritable, Text, Text, IntWritable> {

//	private static BufferedWriter bw;
//	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("Reduce3Out.txt"));
//		BufferedReader br = new BufferedReader(new FileReader("Map3Out.txt"));
//		
//		ArrayList<Text> values = new ArrayList<Text>();
//		String read = "";
//		while ((read = br.readLine()) != null) {
//			values.add(new Text(read));
//		}
//		
//		Reduce3 r = new Reduce3();
//		r.reduce(new IntWritable(1), values, null);
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		NodeBox[] arr = new NodeBox[10];
		int count = 0;
		
		for (Text t: values) {
			count = this.addElement(arr, this.makeNodeBox(t.toString()), count);
		}
		
		for (NodeBox nb: arr) {
			if (context == null) {
				if (nb != null) {
//					bw.write(nb.node + "\t" + nb.significance);
//					bw.newLine();
				}
			} else {
				context.write(new Text(nb.node), new IntWritable(nb.significance));
			}
		}
	}
	
	private int addElement(NodeBox[] arr, NodeBox newEle, int curOccupants) {
		if (curOccupants < 10) {
			arr[curOccupants] = newEle;
			return curOccupants + 1;
		} else {
			int smallestIndex = this.findSmallest(arr);
			if (arr[smallestIndex].significance < newEle.significance) {
				arr[smallestIndex] = newEle;
			}
			return curOccupants;
		}
	}
	
	private int findSmallest(NodeBox[] arr) {
		int smallest = 0;
		
		for (int i = 1; i < arr.length; ++i) {
			if (arr[smallest].significance > arr[i].significance) {
				smallest = i;
			}
		}
		
		return smallest;
	}
	
	private NodeBox makeNodeBox(String string) {
		StringTokenizer st = new StringTokenizer(string);
		return new NodeBox(st.nextToken(), Integer.parseInt(st.nextToken()));
	}

	private static class NodeBox extends Object {
		public String node;
		public int significance;
		
		public NodeBox(String n, int s) {
			this.node = n;
			this.significance = s;
		}
		
		public String toString() {
			return node +" "+significance;
		}
	}
}
