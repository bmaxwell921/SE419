
import java.util.*;
import java.nio.ByteBuffer;

public class MinHashUtil {

	//Static data
	private static int numMinHashes = 6;
	private static int[] bigSeedArray = {42, 17, 100, 7, 13, 21, 91, 72, 65, 75};
	private static int kShingles = 9;

	public static int[] makeMinHashes(String doc) {
		int[] minHashes = new int[numMinHashes];

		List<String> shingles = makeShingles(doc);

		for (int i = 0; i < numMinHashes; ++i) {
			minHashes[i] = makeMinHash(shingles, bigSeedArray[i]);
		}
		return minHashes;
	}

	public static int makeMinHash(List<String> shingles, int seed) {
		int minHash = hash(shingles.get(0).getBytes(), seed);
		for (int i = 1; i < shingles.size(); ++i) {
			int curHash = hash(shingles.get(i).getBytes(), seed);
			if (curHash < minHash) {
				minHash = curHash;
			}
		}
		return minHash;
	}

	public static List<String> makeShingles(String doc) {
		List<String> shingles = new ArrayList<String>();

		for (int i = 0; i < kShingles; ++i) {
			shingles.add(doc.substring(i, i + kShingles));
		}

		return shingles;
	}


	//Lab 4 hash function
	public static int hash(byte[] b_con, int i_seed){

		String content = new String(b_con);

		int seed = i_seed;
		int m = 0x5bd1e995;
		int r = 24;

		int len = content.length();
		byte[] work_array = null;

		int h = seed ^ content.length();

		int offset = 0;

		while( len >= 4)
		{
			work_array = new byte[4];
			ByteBuffer buf = ByteBuffer.wrap(content.substring(offset, offset + 4).getBytes());

			int k = buf.getInt();
			k = k * m;
			k ^= k >> r;
		k *= m;
		h *= m;
		h ^= k;

		offset += 4;
		len -= 4;
		}

		switch(len){
		case 3: h ^= work_array[2] << 16;
		case 2: h ^= work_array[1] << 8;
		case 1: h ^= work_array[0];
		h *= m;
		}

		h ^= h >> 13;
		h *= m;
		h ^= h >> 15;

		return h;
	}
}