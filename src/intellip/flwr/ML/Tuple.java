package intellip.flwr.ML;

import java.util.ArrayList;

/** 
 * An abstract for storing the feature values for any single data point
 */
public class Tuple {

	// ArrayList makes more sense...
	private ArrayList<Double> feature;
	private int TupleSize;
	private int CurrentSize;
	
	// Constructor
	public Tuple (int dimension) {
	
		TupleSize = dimension;
		feature = new ArrayList<Double>(TupleSize);
	}
	
	// This implementation of add() does not complain on out-of-bound issues
	// It simply ignores it
	public boolean add(double d) {
		
		if ( CurrentSize > TupleSize ) return false;
		feature.add(d);
		CurrentSize++;
		return true;
	}
	
	public boolean addAll(ArrayList<Double> data) {
		if ( data.size() != TupleSize) return false;
		return feature.addAll(data);
	}
	
	// set method
	public void set (int position, double value) {
		if ( position >= 0 &&  position < CurrentSize ) {
			feature.set(position, value);
		}
	}
	
	// get method
	public double get (int position) {
		if ( position >= 0 &&  position < CurrentSize ) {
			return feature.get(position);
		}
		return (Double) null;
	}
	
	public int GetDimension () { return TupleSize; }
	
	/* 
	 * Returns the current size of the feature tuple
	 */
	public boolean isFull() { return CurrentSize == TupleSize? true: false; }
	
	public String toString() {
		return feature.toString();
	}
	
	// TEST 
	public static void main(String[] args) {
		
		Tuple row = new Tuple(3);
		row.add(1.0);
		System.out.println(row.isFull());
		row.add(2.0);
		row.add(3.0);
		System.out.println(row.isFull());
		System.out.println(row);
		row.set(1, 4.0);
		System.out.println(row);
		System.out.println(row.get(0));
	}

}
