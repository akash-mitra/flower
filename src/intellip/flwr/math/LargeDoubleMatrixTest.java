package intellip.flwr.math;

import java.io.IOException;

public class LargeDoubleMatrixTest {
    
    public static void getSetMatrix() throws IOException {
        long start = System.nanoTime();
        final long used0 = usedMemory();
        
        LargeDoubleMatrix matrix = new LargeDoubleMatrix("/Users/akash/largeFile.txt", 1000, 1000);
        
        // writing
        for (int i = 0; i < matrix.width(); i++)
            matrix.set(i, i, i);
        
        // reading
        for (int i = 0; i < matrix.width(); i++)
            assert i == matrix.get(i, i);
        
        System.out.println(matrix.get(10, 10));
        System.out.println(matrix.get(100, 10));
        System.out.println(matrix.get(999, 999));
        
        long time = System.nanoTime() - start;
        final long used = usedMemory() - used0;
        if (used == 0)
            System.err.println("You need to use -XX:-UsedTLAB to see small changes in memory usage.");
        System.out.printf("Setting the diagonal took %,d ms, Heap used is %,d KB%n", time / 1000 / 1000, used / 1024);
        matrix.close();
    }
    
    

    private static long usedMemory() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }
    
    public static void main(String[] args) {
    	try {
    		getSetMatrix();
    	} catch(Exception e) { e.printStackTrace(); }
    }
}