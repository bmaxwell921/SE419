package common;

import static org.junit.Assert.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

public class LimitedPriorityArrayTest {

	private LimitedPriorityArrayList<Integer> lpa;
	
	@Before
	public void startUp() {
		lpa = new LimitedPriorityArrayList<Integer>(10, new Comparator<Integer>() {
			@Override
			public int compare(Integer lhs, Integer rhs) {
				return lhs - rhs;
			}
		});
	}
	
	@Test
	public void didntFuckUpFromStart() {
		assertEquals(10, lpa.getMaxCapacity());
		assertEquals(0, lpa.getCurrentCapacity());
		
		assertArrayEquals(new Integer[] {null,null,null,null,null,null,null,null,null,null}, lpa.getAll());
	}
	
	@Test
	public void testAddNormalOne() {
		lpa.add(5);
		
		Integer[] correct = {5, null, null, null, null, null, null, null, null, null};
		assertArrayEquals(correct, lpa.getAll());
		assertEquals(1, lpa.getCurrentCapacity());
		assertEquals(10, lpa.getMaxCapacity());
	}
	
	@Test
	public void testAddNormalLessThanMax() {
		lpa.add(5);
		lpa.add(6);
		lpa.add(7);
		lpa.add(8);
		lpa.add(9);
		
		Integer[] correct = {5, 6, 7, 8, 9, null, null, null, null, null};
		
		assertArrayEquals(correct, lpa.getAll());
		assertEquals(5, lpa.getCurrentCapacity());
		assertEquals(10, lpa.getMaxCapacity());
	}
	
	@Test
	public void testAddNormalMoreThanCapacity() {
		for (int i = 0; i < 15; ++i) {
			lpa.add(i);
		}
		
		Integer[] correct = {10,11,12,13,14,5,6,7,8,9};
		
		assertArrayEquals(correct, lpa.getAll());
		assertEquals(10, lpa.getCurrentCapacity());
		assertEquals(10, lpa.getMaxCapacity());
	}
	
	@Test
	public void testAddNormalMoreThanCapacityNoAdd() {
		for (int i = 0; i < 10; ++i) {
			lpa.add(100);
		}
		
		Integer[] correct = {100,100,100,100,100,100,100,100,100,100};
		assertArrayEquals(correct, lpa.getAll());
		assertEquals(10, lpa.getCurrentCapacity());
		assertEquals(10, lpa.getMaxCapacity());
	}
	
	@Test
	public void testAddNormalReturns() {
		for (int i = 0; i <10; ++i) {
			assertTrue(lpa.add(100));
		}
		
		for (int i = 0; i < 5; ++i) {
			assertFalse(lpa.add(50));
		}
	}
	
	@Test
	public void testNormalAddLotsOfCapacities() {
		Integer[] capacities = {10,20,30,40,50,60,70,80,90,100};
		List<LimitedPriorityArrayList<Integer>> lpas = new ArrayList<LimitedPriorityArrayList<Integer>>();
		
		Comparator<Integer> comp = new Comparator<Integer>() {
			@Override
			public int compare(Integer lhs, Integer rhs) {
				return lhs - rhs;
			}
		};
		for (int i = 0; i < 10; ++i) {
			lpas.add(new LimitedPriorityArrayList<Integer>(capacities[i], comp));
		}
		
		for (int curCap = 0; curCap < capacities.length; ++curCap) {
			int i = capacities[curCap];
			for (int j = 0; j < i; ++j) {
				assertTrue(lpas.get(curCap).add(j));
				assertEquals(j + 1, lpas.get(curCap).getCurrentCapacity());
				assertEquals(i, lpas.get(curCap).getMaxCapacity());
			}
			assertEquals(i, lpas.get(curCap).getCurrentCapacity());
		}
	}
	
	@Test (expected = NoSuchElementException.class)
	public void testRemoveEmpty() {
		lpa.remove();
	}
	
	@Test
	public void testRemoveNormal() {
		lpa.add(5);
		Integer rem = lpa.remove();
		
		assertEquals(new Integer(5), rem);
		assertEquals(0, lpa.getCurrentCapacity());
		assertEquals(10, lpa.getMaxCapacity());
	}

}
