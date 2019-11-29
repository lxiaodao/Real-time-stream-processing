package com.rooibook.sparkdemo;

import java.util.concurrent.atomic.AtomicInteger;

public final class CountPartiton {
	
	private static CountPartiton n=new CountPartiton();
	private CountPartiton() {
		
	}
	public static CountPartiton getInstance() {
		return n;
		
	}
	private static AtomicInteger COUNTER=new AtomicInteger(0);
	
	public synchronized static int count() {
		return COUNTER.incrementAndGet();
	}
	public synchronized static int getCountAll() {
		return CountPartiton.COUNTER.get();
	}
	public synchronized static int resetCount(int num) {
		COUNTER.set(num);
		return 1;
	}

}
