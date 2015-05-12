package com.github.gaoyangthu.ytz.security.sha1;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/10/30 0030
 */
public class SHA1DigesterTest {
	public SHA1DigesterTest() {
		System.out.println("A new SHA1DigesterTest instance.");
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
	public void chechDigester() {
		String content = "20141028王小二330719196804253671";
		String digests = "10d1552ebdde76b7c439b32c8887f9d195f20744";
		System.out.println("摘要结果：" + DigesterTest.digestToString(content));
		System.out.println("比对结果：" + DigesterTest.checkDigest(content, digests));
	}
}
