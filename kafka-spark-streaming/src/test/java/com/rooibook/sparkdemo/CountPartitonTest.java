/**
 * 
 */
package com.rooibook.sparkdemo;

import org.junit.Assert;
import org.junit.Test;



/**
 * @author yangliu
 *
 */
public class CountPartitonTest {
	
	
	@Test
	public void test_resetCounter() {
		
		CountPartiton counteP=CountPartiton.getInstance();
		
		for(int i=0;i<20;i++) {
			counteP.count();
		}
		
		Assert.assertEquals(20, counteP.getCountAll());
		
		CountPartiton.resetCount(2);
		
		Assert.assertEquals(2, counteP.getCountAll());
		
	}

}
