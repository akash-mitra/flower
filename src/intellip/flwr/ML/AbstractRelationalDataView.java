package intellip.flwr.ML;

import java.util.ArrayList;

public class AbstractRelationalDataView {
	
	private long count;
	private ArrayList<Tuple> featureTuples = new ArrayList<Tuple>();
	private ArrayList<Tuple> responseTuples = new ArrayList<Tuple>();
	
	public AbstractRelationalDataView() {
		count = 5;
		
		// add some dummy data
		Tuple xtemp = new Tuple(3);
		Tuple ytemp = new Tuple(1);

		// first data point
		xtemp.add(1.0);
		xtemp.add(1.0);
		xtemp.add(1.0);
		featureTuples.add(xtemp);
		ytemp.add(3.0);
		responseTuples.add(ytemp);
		
		// 2nd data point
		xtemp = new Tuple(3);
		ytemp = new Tuple(1);
		xtemp.add(1.0);
		xtemp.add(3.0);
		xtemp.add(2.0);
		featureTuples.add(xtemp);
		ytemp.add(6);
		responseTuples.add(ytemp);
		
		// 3rd data point
		xtemp = new Tuple(3);
		ytemp = new Tuple(1);
		xtemp.add(1.0);
		xtemp.add(2.0);
		xtemp.add(3.0);
		featureTuples.add(xtemp);
		ytemp.add(6);
		responseTuples.add(ytemp);
		
		// 4th data point
		xtemp = new Tuple(3);
		ytemp = new Tuple(1);
		xtemp.add(1.0);
		xtemp.add(2.0);
		xtemp.add(2.0);
		featureTuples.add(xtemp);
		ytemp.add(5);
		responseTuples.add(ytemp);
		
		// 5th data point
		xtemp = new Tuple(3);
		ytemp = new Tuple(1);
		xtemp.add(1);
		xtemp.add(3);
		xtemp.add(1);
		featureTuples.add(xtemp);
		ytemp.add(5);
		responseTuples.add(ytemp);
		
	}
	public Tuple GetFeatureTuple (int l) {
		return featureTuples.get(l);
	}
	
	public Tuple GetResponse(int l) {
		return responseTuples.get(l);
	}
	
	public long GetCount() {
		return count;
	}
}
