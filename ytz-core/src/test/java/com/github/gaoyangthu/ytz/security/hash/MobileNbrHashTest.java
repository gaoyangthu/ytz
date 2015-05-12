package com.github.gaoyangthu.ytz.security.hash;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/11/28 0028
 */
public class MobileNbrHashTest {
	public MobileNbrHashTest() {
		System.out.println("A new MobileNbrHashTest instance.");
	}

	@Before
	public void setUp() throws Exception {
		System.out.println("Call @Before before a test mechod");
	}

	@After
	public void tearDown() throws Exception {
		System.out.println("Call @After after a test mechod");
	}

	@Test
	public void doEncrypt() {
		String src = "11234567890";
		System.out.println(MobileNbrHash.encryptString(src));
		String des = "17654321890";
		System.out.println(MobileNbrHash.encryptString(des));
	}
}
