package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op aop;
    private HashMap<Object,Integer> aggregate;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.aop = what;
    	this.aggregate = new HashMap<Object,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Object key = getKey(tup.getField(this.gbfield));
    	if (this.aop == Op.COUNT) {
    		if (this.aggregate.get(key) == null) {
    			this.aggregate.put(key, 1);
    		} else {
    			this.aggregate.put(key, this.aggregate.get(key)+1);
    		}
    	}
    }
    
    public Object getKey(Field f) {
    	if (this.gbfield == NO_GROUPING) {
    		if (!this.aggregate.keySet().contains(NO_GROUPING)) {
    			this.aggregate.put(NO_GROUPING, null);
    		}
    		return NO_GROUPING;
    	} if (!this.aggregate.keySet().contains(f)) {
    		this.aggregate.put(f, null);
    	}
    	return f;
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new StringAggregatorIterator();
    }

    public class StringAggregatorIterator implements DbIterator {
    	
    	private static final long serialVersionUID = 1L;
		private boolean open;
    	private ArrayList<Object> keys;
    	int index = 0;
    	
    	public StringAggregatorIterator() {
    		this.open = false;
    		this.keys = new ArrayList<Object>();
    		this.keys.addAll(StringAggregator.this.aggregate.keySet());
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			this.open = true;
			this.index = 0;
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			if (!this.open || this.index >= keys.size()) return false;
			return true;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			if (this.hasNext()) {
				Object field = this.keys.get(this.index);
				Tuple ntuple = new Tuple(this.getTupleDesc());
				if (field instanceof Field) {
					ntuple.setField(0,(Field)field);
					ntuple.setField(1, new IntField((Integer)StringAggregator.this.aggregate.get(field)));
				} else {
					ntuple.setField(0, new IntField((Integer)StringAggregator.this.aggregate.get(field)));
				}
				this.index++;
				return ntuple;
			}
			return null;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			this.index = 0;
		}

		@Override
		public TupleDesc getTupleDesc() {
			Type[] types;
			if (StringAggregator.this.gbfield == Aggregator.NO_GROUPING) {
				types = new Type[]{Type.INT_TYPE};
			} else {
				types = new Type[]{StringAggregator.this.gbfieldtype,Type.INT_TYPE};
			}
			return new TupleDesc(types);
		}

		@Override
		public void close() {
			this.open = false;
		}
    }
}
