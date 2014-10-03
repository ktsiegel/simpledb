package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    /** Holds all of the individual items in this tuple description. */
    private ArrayList<TDItem> tdItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	// make sure the two input arrays are the same length for the input to be legitimate.
    	if (typeAr.length != fieldAr.length) throw new IllegalArgumentException();
    	tdItems = new ArrayList<TDItem>();
    	for (int i=0; i<typeAr.length; i++) {
    		// create new TDItems from the inputed information
    		tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	tdItems = new ArrayList<TDItem>();
    	for (int i=0; i<typeAr.length; i++) {
    		tdItems.add(new TDItem(typeAr[i], null));
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	// check whether the inputed field index references an existing TDItems list index.
    	if (i < 0 || i >= tdItems.size()) throw new NoSuchElementException();
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	// check whether the inputed field index references an existing TDItems list index.
    	if (i < 0 || i >= tdItems.size()) throw new NoSuchElementException();
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	int index = -1;
    	// Iterate through all items and search for one with the given name
    	for (int i=0; i<tdItems.size(); i++) {
    		String fieldName = tdItems.get(i).fieldName;
    		if (fieldName != null && fieldName.equals(name)) {
    			index = i;
    			break;
    		}
    	}
    	// Throw an exception if the name was not found in this tuple description
    	if (index == -1) throw new NoSuchElementException();
        return index;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (int i=0; i<this.numFields(); i++) {
    		size += this.getFieldType(i).getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	ArrayList<Type> types = new ArrayList<Type>();
    	ArrayList<String> names = new ArrayList<String>();
    	// add each tuple description item's type and name to new arrays
    	for (int i=0; i<td1.numFields(); i++) {
    		types.add(td1.getFieldType(i));
    		names.add(td1.getFieldName(i));
    	}
    	for (int j=0; j<td2.numFields(); j++) {
    		types.add(td2.getFieldType(j));
    		names.add(td2.getFieldName(j));
    	}
    	// use these new arrays to create a new tuple description
        return new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (!(o instanceof TupleDesc)) return false;
    	TupleDesc obj = (TupleDesc) o;
    	if (obj.numFields() != this.numFields()) return false;
    	for (int i=0; i<obj.numFields(); i++) {
    		if (!obj.getFieldType(i).equals(this.getFieldType(i))) return false;
    	}
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String tdesc = "";
        for (int i=0; i<this.numFields(); i++) {
        	tdesc += this.getFieldType(i).toString() + "(" + this.getFieldName(i)+ "), ";
        }
        return tdesc.substring(0,tdesc.length()-2);
    }
}
