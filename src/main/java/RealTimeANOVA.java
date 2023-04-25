import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.inference.TestUtils;

public class RealTimeANOVA {
    public static void main(String[] args){
        double[][] full_data = {{42,12},{18,4},{42,3}, {42,4}};
        double[] calorie = {8, 9, 6, 7, 3};
        double[] fat = {2, 4, 3, 5, 1};
        double[] carb = {3, 5, 4, 2, 3};
        double[] control = {2, 2, -1, 0, 3};
        RealTimeANOVA test = new RealTimeANOVA();
        test.calculateAnova(calorie, fat, carb, control);
    }
    public void calculateAnova(double[] calorie, double[] fat,
                               double[]
                                   carb, double[] control){
        List<double[]> classes = new ArrayList<double[]>();
        classes.add(calorie);
        classes.add(fat);
        classes.add(carb);
        classes.add(control);

        System.out.println(TestUtils.oneWayAnovaFValue(classes));
        System.out.println(TestUtils.oneWayAnovaPValue(classes));
        System.out.println(TestUtils.oneWayAnovaTest(classes, 0.05));
    }
}
