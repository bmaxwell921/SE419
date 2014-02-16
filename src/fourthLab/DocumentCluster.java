package fourthLab;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class DocumentCluster {
	private Set<String> hashKeys;
	
	private String repDoc;
	
	private List<Long> documentIds;
	
	private final String KEY = "Key";
	private final String REP_KEY = "RepDoc";
	private final String CONTENTS_KEY = "Contents";
	private final String KEY_VALUE_DELIM = ":";
	private final String TOSTRING_DELIM = ";";
	private final String LIST_DELIM = ",";
	private final float DEFAULT_PERC = 0.5f;
	
	public DocumentCluster(String version) {
		documentIds = new ArrayList<Long>();
		hashKeys = new HashSet<String>();
		/*
		 * This will look like:
		 * Key:123456,654987;RepDoc:helloI'mADocument;Contents:654321,98746541,321,98,15321,
		*/
		StringTokenizer initialSt = new StringTokenizer(version, TOSTRING_DELIM);
		while (initialSt.hasMoreTokens()) {
			StringTokenizer secondarySt = new StringTokenizer(initialSt.nextToken(), KEY_VALUE_DELIM);
			String key = secondarySt.nextToken();
			String value = secondarySt.nextToken();
			if(key.equals(this.KEY)) {
				this.readKeys(value);
			} else if (key.equals(REP_KEY)) {
				repDoc = value;
			} else if (key.equals(CONTENTS_KEY)) {
				this.readDocIds(value);
			}
		}
	}
	
	private void readKeys(String keys) {
		StringTokenizer st = new StringTokenizer(keys, LIST_DELIM);
		while (st.hasMoreTokens()) {
			hashKeys.add(st.nextToken());
		}
	}
	
	private void readDocIds(String docs) {
		StringTokenizer tertiarySt = new StringTokenizer(docs, LIST_DELIM);
		while (tertiarySt.hasMoreTokens()) {
			documentIds.add(Long.parseLong(tertiarySt.nextToken()));
		}
	}
	
	/**
	 * Constructs a new DocumentGroup
	 * @param key
	 * 				A String representation of the keys
	 */
	public DocumentCluster(Text key) {
		//hashKeys = new ArrayList<Set<String>>();
		hashKeys = new HashSet<String>();
		this.readKeys(key.toString());
		this.documentIds = new ArrayList<Long>();
		this.repDoc = null;
	}
	
	/**
	 * Adds a document to the document group. This method will only add a document
	 * if all the hashKeys are the same
	 * @param hashKey
	 * 				the key that does along with this document
	 * @param document
	 * 				the document to be added. Expected to be in the form "id-contents"
	 * @return
	 * 			success or failure of adding
	 */
	public boolean addDocument(String hashKey, String document) {
		if (!this.containsKeys(hashKey)) return false;
		return addDocument(document);
	}
	
	/*
	 * Method to check if all the hash keys are the same
	 */
	private boolean containsKeys(String hashKey) {
		StringTokenizer st = new StringTokenizer(hashKey, LIST_DELIM);
		Set<String> otherKeys = new HashSet<String>();
		while (st.hasMoreTokens()) {
			otherKeys.add(st.nextToken());
		}
		
		return this.hashKeys.contains(otherKeys);
	}
	
	/**
	 * A method to add documents to the document group without checking the key
	 * @param document
	 * 				the document to be added. Expected to be in the form "id-contents"
	 * @return
	 * 			success or failure of adding
	 */
	public boolean addDocument(String document) {
		StringTokenizer st = new StringTokenizer(document, "-");
		documentIds.add(Long.parseLong(st.nextToken()));
		
		if (repDoc == null) {
			repDoc = st.nextToken();
		}
		
		return true;
	}
	
	/**
	 * Method to return the number of documents in this cluster
	 * @return
	 * 		the number of documents in this cluster
	 */
	public int size() {
		return (documentIds == null) ? 0 : documentIds.size();
	}
	
	/**
	 * Method to get the document that represents this cluster
	 * @return
	 * 		the document that represents this cluster
	 */
	public String getRepDoc() {
		return repDoc;
	}
	
	/**
	 * Method to combine two document groups together if they meet some metric. 
	 * Note: other will be empty after this method call 
	 * @param other
	 * 			the other document group
	 * @return
	 * 			true or false depending on whether we could combine the groups
	 */
	public boolean combine(DocumentCluster other) {
		return this.combine(other, DEFAULT_PERC);
	}
	
	/**
	 * Method to combine two document clusters if they have enough matching keys
	 * @param other
	 * 			the other document cluster
	 * @param percentSame
	 * 				the percent of keys that need to be the same to combine
	 * @return
	 * 		true or false depending oh whether we could combine the groups or not
	 */
	public boolean combine(DocumentCluster other, float percentSame) {
		if (this.size() == 0 || other.size() == 0) return false;
		boolean combined = false;
		
		//If percentSame minHashes in the smaller DocumentCluster match, then merge
		//those results into the bigger DocumentCluster
		
		DocumentCluster smaller = (other.size() < this.size()) ? other : this;
		DocumentCluster larger = (other.size() >= this.size()) ? other : this;
		
		if (larger.needToAbsorb(smaller, percentSame)) {
			larger.absorb(smaller);
			smaller.destroy();
			combined = true;
		}
		
		return combined;
	}
	
	private boolean needToAbsorb(DocumentCluster smaller, double percentSame) {
		if (this.size() < smaller.size()) 
			throw new IllegalArgumentException("Tried to check absorbing a larger DocumentCluster");
		
		double numSame = 0;
		int i = 0;
		for (String smallerKey : smaller.hashKeys) {
			for (String largerKey : this.hashKeys) {
				/*
				 * Optimistically say all the rest are matches. If all the rest plus
				 * what we have so far isn't enough to make them merge, then we know
				 * there's no point in checking the rest because we can't possibly
				 * get enough matches to merge
				 */
				if ((numSame + smaller.hashKeys.size() - i) / smaller.hashKeys.size() < percentSame) {
					return false;
				}
				/*
				 * Fast break for if we see a lot of similar minHashes right at the beginning
				 */
				else if (numSame / smaller.hashKeys.size() >= percentSame) {
					return true;
				}
				if (smallerKey.equals(largerKey)) {
					++numSame;
					//Once we find something we are done with the current smallerKey
					break;
				}
			}
			++i;
		}
		return numSame / smaller.hashKeys.size() >= percentSame;
	}
	
	/*
	 * Method to absorb all the keys and ids of another document group
	 */
	private void absorb(DocumentCluster smaller) {
		hashKeys.addAll(smaller.hashKeys);
		documentIds.addAll(smaller.documentIds);
	}
	
	/*
	 * Method to modify the state of this DocumentCluster in such a way that it is seen as empty 
	 */
	private void destroy() {
		hashKeys = null;
		repDoc = null;
		documentIds = null;
	}
	
	public boolean isAlive() {
		return this.size() != 0;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		/*
		 * This will look like:
		 * Key:123456;RepDoc:helloI'mADocument;Contents:654321,98746541,321,98,15321,
		*/
		
		sb.append(KEY).append(KEY_VALUE_DELIM);
		sb.append(this.keysToString());
		
		sb.append(TOSTRING_DELIM);
		
		sb.append(REP_KEY).append(KEY_VALUE_DELIM);
		sb.append((repDoc == null) ? " " : repDoc);
		
		sb.append(TOSTRING_DELIM);
		
		sb.append(CONTENTS_KEY).append(KEY_VALUE_DELIM);
		sb.append(this.docsToString());
		
		return sb.toString();
	}
	
	public String keysToString() {
		if (hashKeys == null) return " ";
		StringBuilder sb = new StringBuilder();
		for (String key: hashKeys) {
			sb.append(key).append(LIST_DELIM);
		}
		return sb.toString();
	}
	
	public String docsToString() {
		if (documentIds == null) return " ";
		StringBuilder sb = new StringBuilder();
		for (Long id: documentIds) {
			sb.append(id).append(LIST_DELIM);
		}
		return sb.toString();
	}
}
