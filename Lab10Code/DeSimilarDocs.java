import com.ibm.streams.operator.*;
import java.util.*;

public class DeSimilarDocs extends AbstractOperator {

	private int startTime = 0;
	private int windowSize = 60;

	//Keys are the comma separated minhashes and the values are document clusters holding the docs
	private Map<String, DocumentCluster> docGroups;

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);

		List<String> params = context.getParameterValues("WindowSize");
		if (params.size() < 1) {
			throw new IllegalArgumentException("WindowSize parameter value wasn't passed");
		}

		this.windowSize = Integer.parseInt(params.get(0));
		docGroups = new HashMap<String, DocumentCluster>();
	}

	public void process(StreamingInput stream, Tuple tuple) throws Exception {
		//This is what I get tuples from
		final StreamingOutput<OutputTuple> output = getOutput(0);

		int curTime = convertToFriendly(tuple.getString("timeStamp"));
		//Initialization stuff
		if (startTime == 0) {
			startTime = curTime;
			addDoc(tuple);
		} else if (curTime <= startTime + windowSize) { //If we are still in the currentWindow
			//Just add the document and move on
			addDoc(tuple);
		} else { //It's time to submit
			submitCurrentWindow(output);

			//Add the tup that cause the window change
			addDoc(tuple);
			startTime = curTime;
		}
	}

	private int convertToFriendly(String time) {
		int ret = 0;
		String[] vals = time.split(":");
		ret += 60 * Integer.parseInt(vals[0]) + Integer.parseInt(vals[1]);
		return ret; 
	}

	//Just does the min-hashes and throws them in the map
	private void addDoc(Tuple toAdd) {
		StringBuilder keySb = new StringBuilder();
		//Creates min-hashes for the document
		for (int minHash : MinHashUtil.makeMinHashes(toAdd.getString("document"))) {
			keySb.append(minHash).append(",");
		}

		String key = keySb.toString();

		//Then puts the document into the cluster that it matches 100% with, or creates a new one
		DocumentCluster cluster = docGroups.get(key);
		if (cluster == null) {
			cluster = new DocumentCluster(toAdd.getString("timeStamp"), toAdd.getString("filePath"));
		}
		cluster.addKeys(key);
		docGroups.put(key, cluster);
	}

	//This should probably do some consolidating
	private void submitCurrentWindow(final StreamingOutput<OutputTuple> output) throws Exception {
		//Ok, so we need to consolidate the document groups, then output them

		//First just get a list of the map for easy iteration
		List<DocumentCluster> docs = new LinkedList<DocumentCluster>(docGroups.values());

		consolidateGroups(docs);

		//Then output them
		for (DocumentCluster doc : docs) {
			OutputTuple curOut = output.newTuple();
			curOut.setString("timeStamp", doc.getTime());
			curOut.setString("repDocPath", doc.getRep());
			output.submit(curOut);
		}

		//Then restart
		docGroups.clear();
	}

	private void consolidateGroups(List<DocumentCluster> docs) {
		boolean combinedAny = true;
		while (combinedAny) {
			combinedAny = false;

			//Try to do any consolidation possible
			for (int i = 0; i < docs.size(); ++i) {
				DocumentCluster first = docs.get(i);
				if (first.isAlive()) {
					for (int j = i +1; j < docs.size(); ++j) {
						DocumentCluster second = docs.get(j);
						if (second.isAlive()) {
							if (first.combine(second)) {
								combinedAny = true;
							}
						}
					}	
				}
			}

			//Remove the dead ones
			for (Iterator<DocumentCluster> iter = docs.iterator(); iter.hasNext(); ) {
				if (!iter.next().isAlive()) {
					iter.remove();
				}
			}
		}
	}

}