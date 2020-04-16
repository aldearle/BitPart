package uk.al_richard.bitpart.mutiThreaded;

import coreConcepts.CountedMetric;
import coreConcepts.Metric;
import dataPoints.cartesian.CartesianPoint;
import searchStructures.VPTree;
import testloads.TestContext;
import testloads.TestContext.Context;
import uk.al_richard.bitpart.referenceImplementation.*;
import uk.al_richard.bitpart.util.OpenBitSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static uk.al_richard.bitpart.util.Filename.getFileName;

public class MainRunnerFile {

	protected static final double RADIUS_INCREMENT = 0.3;
	private static double MEAN_DIST = 1.81;
	protected static double[] ball_radii =  new double[] { MEAN_DIST - 2 * RADIUS_INCREMENT, MEAN_DIST - RADIUS_INCREMENT,
			MEAN_DIST, MEAN_DIST - RADIUS_INCREMENT,
			MEAN_DIST - 2 * RADIUS_INCREMENT };

	// public static double[] ball_radii = new double[] { 0.168, 0.268, 0.318,
	// 0.418, 0.518, 0.618 };

	public static void main(String[] args) throws Exception {

		Context context = Context.euc20;

		boolean fourPoint = false;
		boolean balanced = false;
		int noOfRefPoints = 20;
		int nChoose2 = ((noOfRefPoints - 1) * noOfRefPoints) / 2;
		int noOfBitSets = nChoose2 + (noOfRefPoints * ball_radii.length);

		TestContext tc = new TestContext(context);
		int querySize = (context == Context.colors || context == Context.nasa) ? tc
				.dataSize() / 10 : 1000;
		tc.setSizes(querySize, noOfRefPoints);
		List<CartesianPoint> dat = tc.getData();

		List<CartesianPoint> refs = tc.getRefPoints();
		double threshold = tc.getThresholds()[0];

		List<CartesianPoint> queries = tc.getQueries();
		System.out.println("Date/time\t" + new Date().toString() );
		System.out.println("Class\t" + getFileName() );
		System.out.println("context\t" + context);
		System.out.println("4point\t" + fourPoint);
		System.out.println("balanced\t" + balanced);
		System.out.println("data size\t" + dat.size());
		System.out.println("query size\t" + querySize);
		System.out.println("threshold\t" + threshold);
		System.out.println("refs size\t" + refs.size());
		System.out.println("no of bitsets\t" + noOfBitSets);

		// buildAndQueryVpt(tc);

		RefPointSet<CartesianPoint> rps = new RefPointSet<>(refs, tc.metric());
		List<ExclusionZone<CartesianPoint>> ezs = new ArrayList<>();

		addSheetExclusions(dat, refs, rps, ezs, balanced, fourPoint);
		addBallExclusions(dat, refs, rps, ezs, balanced);

		System.out.println("ezs size:\t" + ezs.size());

		OpenBitSet[] datarep = new OpenBitSet[noOfBitSets];
		buildBitSetData(dat, datarep, rps, ezs);
		queryBitSetData(queries, dat, tc.metric(), threshold, datarep, rps,
				ezs, noOfRefPoints);

	}

	public static void addBallExclusions(List<CartesianPoint> dat, List<CartesianPoint> refs,
										 RefPointSet<CartesianPoint> rps, List<ExclusionZone<CartesianPoint>> ezs, boolean balanced) {
		for (int i = 0; i < refs.size(); i++) {
			List<BallExclusion<CartesianPoint>> balls = new ArrayList<>();
			for (double radius : ball_radii) {
				BallExclusion<CartesianPoint> be = new BallExclusion<>(rps, i, radius);
				balls.add(be);
			}
			if (balanced) {
				balls.get(0).setWitnesses(dat.subList(0, 1000));
				double midRadius = balls.get(0).getRadius();
				double thisRadius = midRadius - ((balls.size() / 2) * RADIUS_INCREMENT);
				for (int ball = 0; ball < balls.size(); ball++) {
					balls.get(ball).setRadius(thisRadius);
					thisRadius += RADIUS_INCREMENT;
				}
			}
			ezs.addAll(balls);
		}
	}

	public static void addSheetExclusions(List<CartesianPoint> dat,
                                          List<CartesianPoint> refs, RefPointSet<CartesianPoint> rps,
                                          List<ExclusionZone<CartesianPoint>> ezs, boolean balanced,
                                          boolean fourPoint) {
		for (int i = 0; i < refs.size() - 1; i++) {
			for (int j = i + 1; j < refs.size(); j++) {
				SheetExclusion<CartesianPoint> se;
				if( fourPoint ) {
					se = new SheetExclusion4p<>(rps, i, j);
				}  else {
					se = new SheetExclusion3p<>(rps, i, j);
				}
				if (balanced) {
					se.setWitnesses(dat.subList(0, 1000));
				}
				ezs.add(se);
			}
		}
	}

	private static void buildAndQueryVpt(TestContext tc) {

		List<CartesianPoint> data = tc.getData();
		data.addAll(tc.getRefPoints());

		CountedMetric<CartesianPoint> cm = new CountedMetric<>(tc.metric());
		VPTree<CartesianPoint> vpt = new VPTree<>(data, cm);
		cm.reset();
		final List<CartesianPoint> queries = tc.getQueries();
		final double t = tc.getThreshold();

		long t0 = System.currentTimeMillis();
		int noOfRes = 0;
		for (CartesianPoint q : queries) {
			List<CartesianPoint> res = vpt.thresholdSearch(q, t);
			noOfRes += res.size();
		}

		System.out.println("vpt");
		System.out.println("dists per query\t" + cm.reset() / queries.size());
		System.out.println("results\t" + noOfRes);
		System.out.println("time\t" + (System.currentTimeMillis() - t0)
				/ (float) queries.size());
	}

	public static <T> void buildBitSetData(List<T> data, OpenBitSet[] datarep,
										   RefPointSet<T> rps, List<ExclusionZone<T>> ezs) {
		int dataSize = data.size();
		for (int i = 0; i < datarep.length; i++) {
			datarep[i] = new OpenBitSet(dataSize);
		}
		for (int n = 0; n < dataSize; n++) {
			T p = data.get(n);
			double[] dists = rps.extDists(p);
			for (int x = 0; x < datarep.length; x++) {
				boolean isIn = ezs.get(x).isIn(dists);
				if (isIn) {
					datarep[x].set(n);
				}
			}
		}
	}

	protected static <T> OpenBitSet doExclusions(List<T> dat, double t,
			OpenBitSet[] datarep, CountedMetric<T> cm, T q, final int dataSize,
			List<Integer> mustBeIn, List<Integer> cantBeIn) {
		if (mustBeIn.size() != 0) {
			OpenBitSet ands = getAndOpenBitSets(datarep, dataSize, mustBeIn);
			if (cantBeIn.size() != 0) {
				/*
				 * hopefully the normal situation or we're in trouble!
				 */
				OpenBitSet nots = getOrOpenBitSets(datarep, dataSize, cantBeIn);
				nots.flip(0, dataSize);
				ands.and(nots);
				return ands;
				// filterContenders(dat, t, cm, q, res, dataSize, ands);
			} else {
				// there are no cantBeIn partitions
				return ands;
				// filterContenders(dat, t, cm, q, res, dataSize, ands);
			}
		} else {
			// there are no mustBeIn partitions
			if (cantBeIn.size() != 0) {
				OpenBitSet nots = getOrOpenBitSets(datarep, dataSize, cantBeIn);
				nots.flip(0, dataSize);
				return nots;
				// filterContenders(dat, t, cm, q, res, dataSize, nots);
			} else {
				// there are no exclusions at all...
				return null;
				// for (T d : dat) {
				// if (cm.distance(q, d) < t) {
				// res.add(d);
				// }
				// }
			}
		}
	}

	public static <T> void filterContenders(List<T> dat, double t,
											CountedMetric<T> cm, T q, Collection<T> res, final int dataSize,
											OpenBitSet results) {

		// System.out.println( "Contenders size = " + results.cardinality());

		for (int i = results.nextSetBit(0); i != -1 && i < dataSize; i = results.nextSetBit(i + 1)) {
			// added i < dataSize since Cuda code overruns datasize in the -
			// don't know case - must be conservative and include, easier than
			// complex bit masking and shifting.
			if (results.get(i)) {
				if (cm.distance(q, dat.get(i)) <= t) {
					res.add(dat.get(i));
				}
			}
		}
	}

	@SuppressWarnings("boxing")
	static OpenBitSet getAndOpenBitSets(OpenBitSet[] datarep,
			final int dataSize, List<Integer> mustBeIn) {
		OpenBitSet ands = null;
		if (mustBeIn.size() != 0) {
			ands = datarep[mustBeIn.get(0)].get(0, dataSize);
			for (int i = 1; i < mustBeIn.size(); i++) {
				ands.and(datarep[mustBeIn.get(i)]);
			}
		}
		return ands;
	}

	@SuppressWarnings("boxing")
	static OpenBitSet getOrOpenBitSets(OpenBitSet[] datarep,
			final int dataSize, List<Integer> cantBeIn) {
		OpenBitSet nots = null;
		if (cantBeIn.size() != 0) {
			nots = datarep[cantBeIn.get(0)].get(0, dataSize);
			for (int i = 1; i < cantBeIn.size(); i++) {
				final OpenBitSet nextNot = datarep[cantBeIn.get(i)];
				nots.or(nextNot);
			}
		}
		return nots;
	}

	@SuppressWarnings("boxing")
	public static <T> void queryBitSetData(List<T> queries, List<T> dat,
										   Metric<T> metric, double threshold, OpenBitSet[] datarep,
										   RefPointSet<T> rps, List<ExclusionZone<T>> ezs, int noOfRefPoints) {

		int noOfResults = 0;
		int partitionsExcluded = 0;
		CountedMetric<T> cm = new CountedMetric<>(metric);
		long t0 = System.currentTimeMillis();
		for (T q : queries) {
			List<T> res = new ArrayList<>();
			double[] dists = rps.extDists(q, res, threshold);

			List<Integer> mustBeIn = new ArrayList<>();
			List<Integer> cantBeIn = new ArrayList<>();

			for (int i = 0; i < ezs.size(); i++) {
				ExclusionZone<T> ez = ezs.get(i);
				if (ez.mustBeOut( dists, threshold)) {
					mustBeIn.add(i);
				} else if (ez.mustBeOut( dists, threshold)) {
					cantBeIn.add(i);
				}
			}

			partitionsExcluded += cantBeIn.size() + mustBeIn.size();

			OpenBitSet inclusions = doExclusions(dat, threshold, datarep, cm,
					q, dat.size(), mustBeIn, cantBeIn);

			// System.out.println("Inclusions = " + inclusions.size());

			if (inclusions == null) {
				for (T d : dat) {
					if (cm.distance(q, d) <= threshold) {
						res.add(d);
					}
				}
			} else {
				filterContenders(dat, threshold, cm, q, res, dat.size(),
						inclusions);
			}

			noOfResults += res.size();
			// System.out.println( "results = " + res.size() );
		}

		System.out.println("bitsets");
		System.out.println("dists per query\t"
				+ (cm.reset() / queries.size() + noOfRefPoints));
		System.out.println("results\t" + noOfResults);
		System.out.println("time\t" + (System.currentTimeMillis() - t0)
				/ (float) queries.size());
		System.out.println("partitions excluded\t"
				+ ((double) partitionsExcluded / queries.size()));
	}
}
