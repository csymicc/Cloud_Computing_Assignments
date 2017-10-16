package pageRank;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class PR {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PageRank PreProcessing"));
		
		JavaRDD<String> lines = sc.textFile(args[0], 1);
		JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, Iterable<String>>() {  
            @Override  
            public Tuple2<String, Iterable<String>> call(String str) {
            	String[] s = str.split("\t");
            	Pattern p = Pattern.compile("<target>(.*?)</target>", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
        		Matcher m = p.matcher(s[3]);
        		HashSet<String> linkset = new HashSet<String>();
        		while(m.find()){
        			String target = m.group().replace("<target>", "").replace("</target>", "");
        			target.trim();
        			linkset.add(target);
        		}
        		linkset.remove(s[1]);
        		List<String> links = new ArrayList<>(linkset);
                return new Tuple2<String, Iterable<String>>(s[1], links);  
            }  
        }).filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, Iterable<String>> s) throws Exception {
				return s._2().iterator().hasNext() && s._1().length() != 0;
			}
		}).cache();
		
		JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
			@Override
			public Double call(Iterable<String> s) {
				return 1.0;
			}
		});
		
		for (int current = 0; current < Integer.parseInt(args[1]); current++) {
			JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(
					new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
				public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s){
					int urlCount = Iterables.size(s._1());
					List<Tuple2<String, Double>> results = new ArrayList<>();
					for (String n : s._1) {
						results.add(new Tuple2<>(n, s._2() / urlCount));
			        }
			        return results.iterator();
				}
			});
		    ranks = contribs.reduceByKey(new Function2<Double, Double, Double>() {
			    @Override
			    public Double call(Double a, Double b) {
			    	return a + b;
			    }
		    }).mapValues(new Function<Double, Double> () {
		    	@Override
		    	public Double call(Double sum) {
		    		return 0.15 + sum * 0.85;
		    	}
		    });
		}
		
		ranks = ranks.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
			@Override
			public Tuple2<Double, String> call(Tuple2<String, Double> s) {
				return new Tuple2<Double, String>(s._2(), s._1());
			}
		}).sortByKey(false).mapToPair(new PairFunction<Tuple2<Double, String>, String, Double>() {
			@Override
			public Tuple2<String, Double> call(Tuple2<Double, String> s) {
				return new Tuple2<String, Double>(s._2(), s._1());
			}
		});
		
		ranks.coalesce(1).saveAsTextFile("/csy/data/result");
		sc.stop();
	}
}
