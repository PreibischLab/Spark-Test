package net.preibisch.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Test
{
	public static void main( String[] args )
	{
		final SparkConf conf = new SparkConf().setAppName( "TestSpark");

		final JavaSparkContext sc = new JavaSparkContext(conf);

		final ArrayList< Integer > jobs = new ArrayList<>();

		for ( int i = 0; i < 100; ++i )
			jobs.add( new Integer( i ) );

		final JavaRDD< Integer > rddJobs = sc.parallelize( jobs );

		final JavaPairRDD< Integer, Double > rdd = rddJobs.mapToPair(
				i -> {
					final double d = i - 5 * Math.PI;
					return new Tuple2<>( i, d );
				});

		/* remember the result after being computed so it's not duplicated */
		rdd.cache();

		/* run it for all jobs */
		rdd.count();

		final List< Tuple2< Integer, Double > > results = rdd.collect();

		for ( final Tuple2< Integer, Double > t : results )
			System.out.println( t._1() + " " + t._2() );

		sc.close();
	}
}
