package common;

import java.util.Collection;
import java.util.Comparator;
import java.util.NoSuchElementException;

/**
 * A class that holds at max the given number of elements. Elements are added normally
 * until the number of elements stored is the max capacity. At that point additional calls
 * to add will remove the smallest element, provided that the new element is not smaller
 * than the smallest element already held. Smallest is defined in terms of the comparator
 * given upon construction.
 * 
 * @author Brandon
 *
 * @param <E>
 */
public class LimitedPriorityArrayList<E> implements LimitedPriorityList<E> {
	
	// Limits the size of the contents
	private int maxCapacity;
	
	//Number of array locations currently used
	private int curOccupants;
	
	//Comparator used to compare objects
	private Comparator<E> comp;
	
	//The stored elements
	private E[] elements;
	
	/**
	 * Constructs a new LimitedPriorityArray with the given maxCapactiy
	 * and will use the given comparator to compare input objects.
	 * @param maxCapacity
	 * 						the maximum number of elements to store at once
	 * @param comp
	 * 				the comparator used to compare elements in the array
	 */
	public LimitedPriorityArrayList(int maxCapacity, Comparator<E> comp) {
		this.maxCapacity = maxCapacity;
		curOccupants = 0;
		this.comp = comp;
		
		elements = (E[]) new Object[maxCapacity];
	}
	
	/**
	 * A method to add an element to the LimitedPriorityArray. This method takes O(n)
	 * time in the worst case. If the new element is equal to the smallest element
	 * then it replaces the smallest element.
	 * @param ele
	 * 				the element to add
	 * @return
	 * 			true if the element was added, false otherwise
	 */
	public boolean add(E ele) {
		if (curOccupants < maxCapacity) {
			elements[curOccupants++] = ele;
			return true;
		} else {
			int smallestIndex = this.findSmallest();
			if (comp.compare(elements[smallestIndex], ele) <= 0) {
				elements[smallestIndex] = ele;
				return true;
			} else {
				return false;
			}
		}
	}
	
	/**
	 * A method to add an entire collection to this object. Adding 
	 * will follow the same rules as the regular add
	 * @param col
	 * @return
	 * 			the number of elements successfully added
	 */
	public int addAll(Collection<E> col) {
		int added = 0;
		for (E e: col) {
			added += (this.add(e)) ? 1 : 0;
		}
		return added;
	}
	
	/**
	 * Method to remove the smallest element currently stored. This method
	 * takes O(n) time.
	 * 
	 * @return
	 * 			the element just removed, or null
	 * @throws NoSuchElementException
	 * 									if there are no elements to remove
	 */
	public E remove() {
		int smallest = this.findSmallest();
		return this.remove(smallest);
	}
	
	/**
	 * A method to remove and return the elemnt at the given position.
	 * This method takes O(1) time.
	 * @param index
	 * 				the index to remove at
	 * @return
	 * 			the element removed
	 * @throws ArrayIndexOutOfBoundsException
	 */
	public E remove(int index) throws ArrayIndexOutOfBoundsException{
		if (index >= curOccupants) throw new ArrayIndexOutOfBoundsException("Index: " + index + " was out of bounds.");
		
		E rem = elements[index];
		this.swap(index, --curOccupants);
		
		return rem;
	}
	
	/**
	 * A method to return an array of the contents of this LimitedPriorityArray
	 * @return
	 * 			a deep copy of the stored elements
	 */
	public E[] getAll() {
		return elements.clone();
	}
	
	/**
	 * A method to return the total amount of objects that can
	 * be stored at once
	 * 
	 * @return
	 * 			the max number of elements that can be store
	 */
	public int getMaxCapacity() {
		return maxCapacity;
	}
	
	/**
	 * A method to get the the number of elements currently 
	 * in this LimitedPriorityArray
	 * 
	 * @return
	 * 			the number of elements currently stored
	 */
	public int getCurrentCapacity() {
		return curOccupants;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		for (int i = 0; i < maxCapacity; ++i) {
			sb.append(elements[i]);
			sb.append((i == maxCapacity - 1) ? "}" : ", ");
		}
		
		return sb.toString();
	}
	
	
	private int findSmallest() {
		if (curOccupants == 0) throw new NoSuchElementException("No elements stored.");
		int smallest = 0;
		
		for (int i = 1; i < curOccupants; ++i) {
			if (comp.compare(elements[i], elements[smallest]) < 0) {
				smallest = i;
			}
		}
		
		return smallest;
	}
	
	private void swap(int i, int j) {
		E temp = elements[i];
		elements[i] = elements[j];
		elements[j] = temp;
	}
	
}
