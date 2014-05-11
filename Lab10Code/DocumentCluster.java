
import java.util.*;

public class DocumentCluster {

	private static float DEFAULT_PERC = 0.25f;

	//The keys associated with this DocumentCluster
	private Set<String> keys;

	//The document representing this cluster
	private String repDoc;

	//The time associated with the repDoc
	private String time;

	private boolean isAlive;

	public DocumentCluster(String time, String repDoc) {
		this.time = time;
		this.repDoc = repDoc;
		keys = new HashSet<String>();
		isAlive = true;
	}

	public String getTime() {
		return time;
	}

	public String getRep() {
		return repDoc;
	}

	public boolean isAlive() {
		return isAlive;
	}

	private void violentlyDestroy() {
		isAlive = false;
	}

	/**
	*	Takes the csv version of the keys and slaps them on into the keys associated with this DC
	*/
	public void addKeys(String csvKeys) {
		StringTokenizer st = new StringTokenizer(csvKeys, ",");
		while (st.hasMoreTokens()) {
			keys.add(st.nextToken());
		}
	}

	public int size() {
		return keys.size();
	}

	public boolean combine(DocumentCluster other) {
		if (!this.isAlive() || !other.isAlive()) return false;
		boolean combined = false;

		//If percentSame minHashes in the smaller DocumentCluster match, then merge
		//those results into the bigger DocumentCluster
		
		DocumentCluster smaller = (other.size() < this.size()) ? other : this;
		DocumentCluster larger = (other.size() >= this.size()) ? other : this;

		if (larger.needToAbsorb(smaller)) {
			larger.absorb(smaller);
			smaller.violentlyDestroy();
			combined = true;
		}
		return combined;
	}

	private boolean needToAbsorb(DocumentCluster smaller) {
		float numSame = 0;
		for (String smallKey : smaller.keys) {
			if (keys.contains(smallKey)) {
				++numSame;
			}
		}

		return (numSame / smaller.keys.size()) >= DEFAULT_PERC;
	}

	private void absorb(DocumentCluster smaller) {
		keys.addAll(smaller.keys);
	}
}