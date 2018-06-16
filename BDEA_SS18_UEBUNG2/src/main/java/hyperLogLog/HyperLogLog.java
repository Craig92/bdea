package hyperLogLog;

import org.junit.Test;

public class HyperLogLog {

	public static int[] buckets = new int[1024];

	@Test
	public void test() {
		String[] test = new String[] { "1101100011100110", "1111100011001100", "0000110001100110", "1101100001111010" };
		for (String input : test) {
			buckets[getBucketNr(input)] = getNumberOfZero(input);
			System.out.println("Bucket " + getBucketNr(input) + " : " + getNumberOfZero(input));
		}

		System.out.println("HyperLogLog: E = " + hyperLogLogFunction() / buckets.length + " %");
		System.out.println("LogLog:      E = " + logLogFunction() / buckets.length + " %");

	}

	public static void main(String args[]) {

		String input;

		// befüllen der Buckets mit zufälligen Werten
		for (int i = 0; i < 1000; i++) {
			input = generateRandomHash();
			buckets[getBucketNr(input)] = getNumberOfZero(input);
			System.out.println("Bucket " + getBucketNr(input) + " : " + getNumberOfZero(input));
		}

		// Ergebnisse
		System.out.println("HyperLogLog: E = " + hyperLogLogFunction() / buckets.length + " %");
		System.out.println("LogLog:      E = " + logLogFunction() / buckets.length + " %");

	}

	/**
	 * Generate a random hash with 16 bits
	 * 
	 * @return the hashcode
	 */
	public static String generateRandomHash() {
		String ret = "";
		for (int i = 0; i < 16; i++) {
			if (Math.random() >= 0.5)
				ret += "1";
			else
				ret += "0";
		}
		System.out.println("Hash: " + ret);
		return ret;
	}

	/**
	 * get the bucket number
	 * 
	 * @param hash
	 *            the handed hash
	 * @return the numer of the bucket
	 */
	public static int getBucketNr(String hash) {
		return Integer.parseInt(hash.substring(0, 6), 2);
	}

	/**
	 * get the numbers of zero int the hash
	 * 
	 * @param hash
	 *            the handed hash
	 * @return number of zero
	 */
	public static int getNumberOfZero(String hash) {

		int result = 0;
		char letter;
		for (int i = 6; i != 16; i++) {
			letter = hash.charAt(i);
			if (letter == '1') {
				return result;
			} else {
				result++;
			}
		}
		return result;
	}

	/**
	 * Calculate the hyperloglog result
	 * 
	 * @return the result
	 */
	public static double hyperLogLogFunction() {

		double am = 0.783;
		double m = buckets.length;
		double sum = 0;

		for (int i = 0; i < buckets.length; i++) {
			sum += Math.pow(2, -buckets[i]);
		}

		return (am * (m * m)) / sum;

	}

	/**
	 * Calculate the loglog result
	 * 
	 * @return the result
	 */
	public static double logLogFunction() {

		double am = 0.783;
		double m = buckets.length;
		double sum = 0;

		for (int i = 0; i < buckets.length; i++) {
			sum += buckets[i];
		}

		return am * (m * (Math.pow(2, ((1 / m) * sum))));

	}
}
