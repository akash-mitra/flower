package intellip.flwr.ML;

public class GradientDescent {

	public static void main(String[] args) {
			
			// set data source
			AbstractRelationalDataView dataSource = new AbstractRelationalDataView();
			
			// set GD algorithm
			GDScalarParameterOptimizer engine = new GDScalarParameterOptimizer();
			engine.setSource(dataSource);
			engine.SetLearnRate(0.2);
			
			// initiate some initial theta values
			Tuple theta = new Tuple(3);
			theta.add(0);
			theta.add(0);
			theta.add(0);
			
			// optimize theta 
			int iteration = 10;
			while ( iteration != 0 ) {
				
				Tuple new_theta = new Tuple(3);
				new_theta = engine.optimize(theta);
				theta = new_theta;
				System.out.println(engine.GetCurrentCost());
				iteration--;
			}
			System.out.println(theta);
		}
}
