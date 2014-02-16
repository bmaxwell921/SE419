package common;

import java.util.Collection;

public interface LimitedPriorityList<E> {

	/**
	 * A method to add an item to the list
	 * @param item
	 * 				the item to add
	 * @return
	 * 			true if it was added, false otherwise
	 */
	public boolean add(E item);
	
	/**
	 * A method to remove and return the "top" element of the list 
	 * @return
	 * 			the removed element
	 */
	public E remove();
	
	/**
	 * A method to remove an element from the list
	 * @param i
	 * 			the element to remove at
	 * @return
	 * 			the removed element
	 */
	public E remove(int i);
	
	/**
	 * A method to get all of the elements in the list 
	 * @return
	 * 			an array containing all the elements in the list
	 */
	public E[] getAll();
	
	/**
	 * A method to add all of the elements in the given collection
	 * @param others
	 * 				all the elements to add
	 * @return
	 * 			the number of elements successfully added
	 */
	public int addAll(Collection<E> others);	
	
	/**
	 * A method to return the total number of items in the list
	 * @return
	 * 			the number of items in the list
	 */
	public int getCurrentCapacity();
	
	/**
	 * A method to get the maximum amount of elements allowed in the list
	 * @return
	 * 			the max number of elements allowed
	 */
	public int getMaxCapacity();
	
}
