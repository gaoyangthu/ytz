package com.github.gaoyangthu.ytz.security.hash;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/11/28 0028
 */
public class MobileNbrHash {
	/*private static int NBR_MASK = 100000;

	public static long encrypt(long nbr) {
		int HIGH_MASK = 0X1FF00;
		int LOW_MASK = 0XFF;
		long head = nbr / NBR_MASK;
		long tail = nbr % NBR_MASK;
		return head * NBR_MASK + (((tail & HIGH_MASK) >> 8) | ((tail & LOW_MASK) << 9));
	}

	public static long decrypt(long nbr) {
		int HIGH_MASK = 0X1FE00;
		int LOW_MASK = 0X1FF;
		long head = nbr / NBR_MASK;
		long tail = nbr % NBR_MASK;
		return head * NBR_MASK + (((tail & HIGH_MASK) >> 9) | ((tail & LOW_MASK) << 8));
	}*/

	/**
	 * Encrypt a mobile phone number for redis indexing.
	 * The implementation reverse the number from 2nd to 8th from left.
	 *
	 * @param nbr which type is string
	 * @return encrypted string
	 */
	public static String encryptString(String nbr) {
		StringBuilder mixed = new StringBuilder("");
		if (nbr.length() == 11) {
			mixed.append(nbr.charAt(0)).append(new StringBuilder(nbr.substring(1, 8)).reverse()).append(nbr.substring(8, 11));
			return mixed.toString();
		} else {
			return nbr;
		}
	}

	/**
	 * Encrypt a mobile phone number for redis indexing.
	 * The implementation reverse the number from 2nd to 8th from left.
	 *
	 * @param nbr which type is long integer
	 * @return encrypted integer
	 */
	public static long encryptLong(long nbr) {
		String tmp = String.valueOf(nbr);
		StringBuilder mixed = new StringBuilder("");
		if (tmp.length() == 11) {
			mixed.append(tmp.charAt(0)).append(new StringBuilder(tmp.substring(1, 8)).reverse()).append(tmp.substring(8, 11));
			return Long.parseLong(mixed.toString());
		} else {
			return nbr;
		}
	}
}
