package fourthLab;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce2 extends Reducer<IntWritable, Text, Text, Text>{

	private static BufferedWriter bw;
	
//	public static void main(String[] args) throws IOException, NumberFormatException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("src/fourthLab/Reduce2Out.txt"));
//		
//		Map<String, ArrayList<String>> values = MapReduceUtil.readReduce("src/fourthLab/Map2Out.txt");
//		
//		Reduce2 r = new Reduce2();
//		
//		for (String s: values.keySet()) {
//			r.reduce(new IntWritable(Integer.parseInt(s)), MapReduceUtil.stringListToTextList(values.get(s)), null);
//		}
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		List<DocumentCluster> groups = this.readDocumentGroups(values);
		this.writeGroups(null, groups);
		
		Info info = this.consolidateGroups(groups);
		
		this.writeGroups(context, groups);
		this.writeInfo(info, context);
	}

	private void writeInfo(Info info, Context context) throws IOException, InterruptedException {
		if (context != null) {
			context.write(new Text(""), new Text("Initial number of clusters: " + info.initialNumClusters));
			context.write(new Text(""), new Text("Initial number of documents: " + info.initialNumDocs));
			context.write(new Text(""), 
					new Text("Initial average number of docs per cluster: " + info.initialAvgDocsPerCluster));
			
			context.write(new Text(""), new Text("Final number of clusters: " + info.finalNumClusters));
			context.write(new Text(""), new Text("Final number of documents: " + info.finalNumDocs));
			context.write(new Text(""), 
					new Text("Final average number of docs per cluster: " + info.finalAvgDocsPerCluster));
		} else {
			System.out.println("Initial number of clusters: " + info.initialNumClusters);
			System.out.println("Initial number of documents: " + info.initialNumDocs);
			System.out.println("Initial average number of docs per cluster: " + info.initialAvgDocsPerCluster);
			
			System.out.println("Final number of clusters: " + info.finalNumClusters);
			System.out.println("Final number of documents: " + info.finalNumDocs);
			System.out.println("Final average number of docs per cluster: " + info.finalAvgDocsPerCluster);	
		}
	}

	private List<DocumentCluster> readDocumentGroups(Iterable<Text> values) {
		//Linked list to support constant time removal
		List<DocumentCluster> ret = new LinkedList<DocumentCluster>();
		
		for (Text t: values) {
			ret.add(new DocumentCluster(t.toString()));
		}
		
		return ret;
	}
	

	private Info consolidateGroups(List<DocumentCluster> groups) {
		Info info = new Info();
		this.gatherInfo(info, groups, true);
		//Iterate thru all the clusters and try to merge them
		boolean combinedAny = true;
		while (combinedAny) {
			combinedAny = false;
			for (DocumentCluster first: groups) {
				/*
				 * Optimization to not check dead documents because they will never
				 * be combined
				 */
				
				if (first.isAlive()) {
					for (DocumentCluster second: groups) {
						if (first != second && second.isAlive()) {
							if(first.combine(second)) {
								combinedAny = true;
							}
						}
					}
				}
			}
			this.removeEmptyGroups(groups);
		}
		
		this.gatherInfo(info, groups, false);
		
		
		return info;
	}
	
	private void gatherInfo(Info info, List<DocumentCluster> groups, boolean isInitial) {
		if (isInitial) {
			info.initialNumClusters = groups.size();
			info.initialNumDocs = this.countDocs(groups);
			info.initialAvgDocsPerCluster = info.initialNumDocs / info.initialNumClusters;
		} else {
			info.finalNumClusters = groups.size();
			info.finalNumDocs = this.countDocs(groups);
			info.finalAvgDocsPerCluster = info.finalNumDocs / info.finalNumClusters;
		}
	}

	private int countDocs(List<DocumentCluster> groups) {
		int sum = 0;
		for (DocumentCluster dc: groups) {
			sum += dc.size();
		}
		return sum;
	}

	private void removeEmptyGroups(List<DocumentCluster> groups) {
		Iterator<DocumentCluster> iter = groups.iterator();
		while (iter.hasNext()) {
			if (!iter.next().isAlive()) {
				iter.remove();
			}
		}
	}

	private void writeGroups(Context context, List<DocumentCluster> groups) throws IOException, InterruptedException {
		for (DocumentCluster dg: groups) {
			if (context == null) {
				bw.write(dg.docsToString() + "\t" + dg.getRepDoc());
				bw.newLine();
			} else {
				context.write(new Text(dg.docsToString()), new Text(dg.getRepDoc()));
			}
		}
	}
	
	private class Info {
		public double initialNumClusters;
		public double initialAvgDocsPerCluster;
		public double initialNumDocs;
		
		public double finalNumClusters;
		public double finalAvgDocsPerCluster;
		public double finalNumDocs;		
	}
}
