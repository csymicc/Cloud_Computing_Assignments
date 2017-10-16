package pageRank;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class FindU {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PageRank U"));
		
		JavaRDD<String> lines = sc.textFile(args[0], 1);
		JavaRDD<String> university = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception{
				return s.indexOf("university") != -1 || s.indexOf("University") != -1;
			}
		});
		
		university.coalesce(1).saveAsTextFile("/csy/data/University");
		
		JavaRDD<String> alumni = lines.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String s) throws Exception{
				return s.indexOf("alumni") != -1 || s.indexOf("Alumni") != -1;
			}
		});
	
		
		alumni.coalesce(1).saveAsTextFile("/csy/data/alumni");
		sc.close();
	}
}
