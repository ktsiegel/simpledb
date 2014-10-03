package simpledb;

import java.io.Serializable;

/**
 * Each instance of BPlusTreeEntry stores one key and two child page ids. It is used
 * by BPlusTreeInternalPage as an abstraction to iterate through the entries stored inside. 
 * All of the entries or tuples in the left child page should be less than or equal to 
 * the key, and all of the entries or tuples in the right child page should be greater 
 * than or equal to the key.
 *
 * @see BPlusTreeInternalPage
 *
 */
public class BPlusTreeEntry implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/**
	 * The key of this entry
	 * */
	private Field key;

	/**
	 * The left child page id
	 * */
	private BPlusTreePageId leftChild;

	/**
	 * The right child page id
	 * */
	private BPlusTreePageId rightChild;

	/**
	 * The record id of this entry
	 * */
	private RecordId rid; // null if not stored on any page

	/**
	 * Constructor to create a new BPlusTreeEntry
	 * @param key - the key
	 * @param leftChild - page id of the left child
	 * @param rightChild - page id of the right child
	 */
	public BPlusTreeEntry(Field key, BPlusTreePageId leftChild, BPlusTreePageId rightChild) {
		this.key = key;
		this.leftChild = leftChild;
		this.rightChild = rightChild;
	}
	
	/**
	 * @return the key
	 */
	public Field getKey() {
		return key;
	}
	
	/**
	 * @return the left child page id
	 */
	public BPlusTreePageId getLeftChild() {
		return leftChild;
	}
	
	/**
	 * @return the right child page id
	 */
	public BPlusTreePageId getRightChild() {
		return rightChild;
	}
	
	/**
	 * @return the record id of this entry, which may be null if this entry is not 
	 * stored on any page
	 */
	public RecordId getRecordId() {
		return rid;
	}
	
	/**
	 * set the key for this entry
	 * @param key - the new key
	 */
	public void setKey(Field key) {
		this.key = key;
	}
	
	/**
	 * set the left child id for this entry
	 * @param leftChild - the new left child
	 */
	public void setLeftChild(BPlusTreePageId leftChild) {
		this.leftChild = leftChild;
	}
	
	/**
	 * set the right child id for this entry
	 * @param rightChild - the new right child
	 */
	public void setRightChild(BPlusTreePageId rightChild) {
		this.rightChild = rightChild;
	}
	
	/**
	 * set the record id for this entry
	 * @param rid - the new record id
	 */
	public void setRecordId(RecordId rid) {
		this.rid = rid;
	}
	
}

