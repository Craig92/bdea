package hyperLogLog;

import java.text.DecimalFormat;

public class HLL {

	static int numberOfevents = 16000;
	static int bucketSize = 1024;
	static int hashLength = 32;

	public static int count = 0;

	public static void main(String args[]) {
		while (doAlgorithms()) {

		}
	}

	public static boolean doAlgorithms() {
		System.out.println();
		System.out.println("run is " + ++count);
		String input;

		int[] buckets = new int[bucketSize];
		int[] bucketsHLL = new int[bucketSize];

		for (int i = 0; i < numberOfevents; i++) {
			input = generateRandomHash();

			int bucketNr = getBucketNr(input);

			// LogLog
			int nrZeros = getNrZeros(input);

			if (nrZeros > buckets[bucketNr])
				buckets[bucketNr] = nrZeros;

			// HyperLogLog
			int nrZerosHLL = getNrZerosHll(input);

			if (nrZerosHLL > bucketsHLL[bucketNr])
				bucketsHLL[bucketNr] = nrZerosHLL;

		}

		double sumLL = 0;

		double sumHLL = 0;

		for (int i = 0; i < bucketSize; i++) {
			sumLL += buckets[i];

			sumHLL += Math.pow(2, -bucketsHLL[i]);
		}

		// output
		System.out.println("Number of events " + numberOfevents);
		System.out.println("sum for loglog: " + sumLL);

		System.out.println("sum for hll: " + sumHLL);

		double valueLL = 0.783 * bucketSize * Math.pow(2, sumLL / bucketSize);

		double valueHLL = (0.783 * bucketSize * bucketSize) / sumHLL;

		System.out.println("e value of ll: " + valueLL);
		System.out.println("e value of hll: " + valueHLL);

		DecimalFormat df = new DecimalFormat("#.##");

		System.out.println("Error ll: " + df.format(((valueLL / numberOfevents) - 1) * 100) + "%");
		System.out.println("Error hll: " + df.format(((valueHLL / numberOfevents) - 1) * 100) + "%");

		// repeat if more than 10% error
		if (numberOfevents / 20 < Math.abs(numberOfevents - valueHLL)
				|| numberOfevents / 20 < Math.abs(numberOfevents - valueLL))
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
		for (int i = 10; i < hashLength; i++) {
			letter = hash.charAt(i);
			if (letter == '1') {
				return (i - 10);
			}
		}
		return 0;
	}

	public static int getNrZerosHll(String hash) {
		char letter;
		for (int i = 10; i < hashLength; i++) {
			letter = hash.charAt(i);
			if (letter == '1') {
				return (i - 9);
			}
		}
		return 0;
	}

	public static int getBucketNr(String hash) {
		String bucketBinary = hash.substring(0, 10);

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
