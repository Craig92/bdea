package hyperLogLog;

public class HyperLogLog {

	public static void main(String args[]) {

		String input;
		int[] buckets = new int[64];

		for (int i = 0; i < 10; i++) {
			input = generateRandomHash();
			System.out.println(input);
			int bucketNr = getBucketNr(input);
			int nrZeros = getNrZeros(input);
			buckets[bucketNr] = nrZeros;
		}

		double sumLL = 0;
		double sumHLL = 0;

		for (int i = 0; i < 64; i++) {
			System.out.println(buckets[i]);
			sumLL += buckets[i];
			sumHLL += Math.pow(2, -buckets[i]);
		}

		System.out.println(sumLL);
		System.out.println(sumHLL);

		double valueLL = 0.783 * 64 * Math.pow(2, (1 / 64) * sumLL);
		double valueHLL = (0.783 * 64 * 64) / sumHLL;

		System.out.println(valueLL);
		System.out.println(valueHLL);

	}

	/**
	 * 
	 * @return
	 */
	public static String generateRandomHash() {
		String ret = "";
		for (int i = 0; i < 32; i++) {
			if (Math.random() >= 0.5)
				ret += "1";
			else
				ret += "0";
		}
		return ret;
	}

	/**
	 * 
	 * @param hash
	 * @return
	 */
	public static int getNrZeros(String hash) {
		char letter;
		for (int i = 6; i < 32; i++) {
			letter = hash.charAt(i);
			if (letter == '1')
				return (i - 6);
		}
		return 0;
	}

	/**
	 * 
	 * @param hash
	 * @return
	 */
	public static int getBucketNr(String hash) {
		String bucketBinary = hash.substring(0, 5);
		return Integer.parseInt(bucketBinary, 2);
	}

	/**
	 * 
	 * @param word
	 * @return
	 */
	public static String hashfunction(String word) {
		String hash;
		return null;
	}
}
