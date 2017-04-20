package kmeans;
import java.util.ArrayList;
import java.lang.Integer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.StringBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.ArrayRealVector



public class DataPoint implements WritableComparable {

    private ArrayRealVector point = null;
    private double cost = 0;
    private int dim = 0;
    public DataPoint() {
    }
    
    public DataPoint(String s) {
        String[] data = s.split("\\s+");
        point = new ArrayRealVector(data.length());
        for(String d: data){
            point.append(Double.valueOf(d));
        }
        dim = point.getDimension();
    }

    public DataPoint(double cost) {
        String[] data = s.split("\\s+");
        this.cost = cost;
        dim = 0 ;
    }
     

    @Override 
    public void write(DataOutput out) throws IOException {
        out.writeDouble(cost);
        out.write(dim);
        for(int i = 0; i< dim; i++){
           out.writeDouble(point.getEntry(i));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cost = in.readDouble();
        dim = in.readInt();
        for(int i = 0; i < dim; i++){
            point.append(in.readDouble());
        }
    }
 
    @Override
    public String toString(){
        StringBuilder s = new StringBuilder();
        for(int i = 0; i < dim; i++){
            s.append(point.getEntry(i));
            s.append("\t");
        }
        return s;
        
    }
    @Override
    public int compareTo(Object o) {
       return -1;
    }

    public int getDim(){
        return dim;
    }
    public double getCost(){
        return cost;
    }
    public RealVector getPoint(){
        return point;
    }
    public double getL1Distance(DataPoint p2){
        return point.getL1Distance(p2.getPoint());
    }
    public double getL2Distance(DataPoint p2){
        return point.getL2Distance(p2.getPoint());
    }
}
