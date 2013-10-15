package intellip.flwr.ML;

public class GDScalarParameterOptimizer {
	
	private int dimensionality;
	private long members;
	private double alpha;
	private AbstractRelationalDataView dv;
	private double currentCost;
	
	public GDScalarParameterOptimizer() { 
		
		// set default learn rate
		alpha = 0.1; 
	}
	
	// set data source view
	public void setSource (AbstractRelationalDataView v) { 
		
		dv = v; 
		
		// obtain number of records in training set
		members = dv.GetCount();
	}
	
	// optional - set learning rate
	public void SetLearnRate (double alpha) { this.alpha = alpha; }
	
	// optimize() take 'theta' as input and returns another set of 'theta'
	// having somewhat tuned values
	public Tuple optimize (Tuple theta) {
		
		// obtain the dimensionality
		dimensionality               = theta.GetDimension();
		
		// check that dimensionality matches with feature dimensions
		// TODO
		
		// scaling constant k = (alpha / m);
		double c                     = alpha / members;
		
		// Start calculating for each theta
		Tuple new_theta = new Tuple(dimensionality);
		
		// Below loop of 'j' will iterate over each parameter
		for (int j = 0; j < dimensionality; j++) {

			double weighted_error    = 0;
			
			// Below loop of 'i' will iterate over each data points of given training set 
			for (int i = 0; i < members; i++) {
				Tuple ithFeatureSet  = dv.GetFeatureTuple(i);
				Tuple ithResponse    = dv.GetResponse(i);
				
				double hypo = 0;
				for (int k = 0; k < dimensionality; k++)
					hypo            += ithFeatureSet.get(k) * theta.get(k);
				double error         = ( hypo - ithResponse.get(0) );
				weighted_error      += error * ithFeatureSet.get(j);	
				currentCost         += error * error;
				
				// TODO LOGGING
				// System.out.println("[" + j + ", " + i + "] For Theta(" + j + "), Row = " + i + ", Feature: " + ithFeatureSet + ", Theta: " + theta + ", Response: " + ithResponse + ", hypo = " + hypo + ", err = " + error + ", weighted_err = " + weighted_error);
				// System.out.println("theta = " + j + ", row = " + i + ", hypo = " + hypo + ", error = " + error + ", weighted err = " + weighted_error);
			}   // end of 'i'
			
			double adjustment        = c * weighted_error;
			currentCost              = currentCost / (2 * members);
			
			// TODO LOGGING
			// System.out.println("alpha = " + alpha + ", m = " + members + ", c = " + c + ", weighted_err = " + weighted_error + ", adj = " + adjustment);
			
			new_theta.add(theta.get(j) - adjustment );
		} // end of 'j'
		
		return new_theta;
	}
	
	public double GetCurrentCost() {
	
		return currentCost;
	}
	
	
}

