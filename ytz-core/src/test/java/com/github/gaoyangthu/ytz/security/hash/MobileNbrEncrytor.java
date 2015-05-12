package com.github.gaoyangthu.ytz.security.hash;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/12/1 0001
 */
public class MobileNbrEncrytor {
	public static long encrypt(long nbr) {
		String tmp = String.valueOf(nbr);
		StringBuilder mixed = new StringBuilder("");
		if (tmp.length() == 11) {
			mixed.append(tmp.charAt(0)).append(new StringBuilder(tmp.substring(1, 8)).reverse()).append(tmp.substring(8, 11));
			return Long.parseLong(mixed.toString());
		} else {
			return nbr;
		}
	}

	public static void main(String[] args) {
		long src = 16777216800L;
		long des = encrypt(src);
		System.out.println(des);
	}
}
