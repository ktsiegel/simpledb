package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /** The schema of this tuple */
    private TupleDesc schema;
    
    /** The location on disk of this tuple */
    private RecordId diskLoc;
    
    /** The fields within this tuple */
    private Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	this.schema = td;
    	this.diskLoc = null;
    	this.fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.diskLoc;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
    	this.diskLoc = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String tupleStr = "";
        for (int i=0; i<fields.length; i++) {
        	tupleStr += fields[i].toString() + "\t";
        }
        return tupleStr.substring(0,tupleStr.length()-1) + "\n";
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return Arrays.asList(fields).iterator();
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td) {
    	this.schema = td;
    }
    
    /**
     * merge two Tuples
     */
    public Tuple merge(Tuple other) {
    	Tuple merged = new Tuple(TupleDesc.merge(this.schema, other.schema));
    	Iterator<Field> it1 = this.fields();
    	Iterator<Field> it2 = other.fields();
    	int index = 0;
    	while (it1.hasNext()) {
    		merged.setField(index, it1.next());
    		index++;
    	}
    	while (it2.hasNext()) {
    		merged.setField(index, it2.next());
    		index++;
    	}
    	return merged;
    }
    
    
}
