package hyperLogLog;

public class HLL {

	static int numberOfevents = 160000;
	static int bucketSize = 1024;
	static int hashLength = 32;

	public static int count;

	public static void main(String args[]) {
		while (doAlgorithms())
			;
	}

	public static boolean doAlgorithms() {
		count = 0;
		String input;

		int[] buckets = new int[bucketSize];

		for (int i = 0; i < numberOfevents; i++) {
			input = generateRandomHash();
			// input = hashfunction(input);
			// System.out.println(input);

			int bucketNr = getBucketNr(input);

			int nrZeros = getNrZeros(input);

			buckets[bucketNr] += nrZeros;

		}
		System.out.println("count is " + count);
		double sumLL = 0;

		double sumHLL = 0;

		for (int i = 0; i < 64; i++) {
			sumLL += buckets[i];

			sumHLL += Math.pow(2, -buckets[i]);
		}
		System.out.println();
		System.out.println("testvalue " + Math.pow(2, sumLL / bucketSize));
		System.out.println();
		System.out.println("sum for loglog: " + sumLL);

		System.out.println("sum for hll: " + sumHLL);

		double valueLL = 0.783 
				//* bucketSize 
				* Math.pow(2, sumLL / bucketSize);

		double valueHLL = (0.783 * bucketSize * bucketSize) / sumHLL;

		System.out.println("e value of ll: " + valueLL);
		System.out.println("e value of hll: " + valueHLL);
		
		if (numberOfevents / 3 < Math.abs(numberOfevents - valueHLL)
				&& numberOfevents / 3 < Math.abs(numberOfevents- valueLL)
				)
			return true;

		return false;
	}

	public static String generateRandomHash() {
		String ret = "";
		for (int i = 0; i < hashLength; i++) {
			if (Math.random() <= 0.5)
				ret += "1";
			else
				ret += "0";
		}
		return ret;
	}

	public static int binaryToInt(String binary) {
		int sum = 0;
		int length = binary.length();
		for (int i = 0; i < length; i++) {
			if (binary.charAt(i) == '1') {
				sum += Math.pow(2, length - i - 1);
			}
		}
		return sum;
	}

	public static int getNrZeros(String hash) {
		char letter;
		for (int i = 8; i < hashLength; i++) {
			letter = hash.charAt(i);
			if (letter == '1') {
				return (i - 8);
			}

			// -5 for loglog
		}
		return 0;
	}

	public static int getBucketNr(String hash) {
		String bucketBinary = hash.substring(0, 10);
		if (Integer.parseInt(bucketBinary, 2) % 2 == 0)
			count++;

		return Integer.parseInt(bucketBinary, 2);
	}

	public static String hashfunction(String word) {
		String hash = Integer.toBinaryString(word.hashCode());
		while (hash.length() <= hashLength) {
			hash = hash + hash;
		}
		return hash.substring(0, hashLength);
	}
}
