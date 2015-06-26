package pku.edu.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.recommender.ClusterSimilarity;
import org.apache.mahout.cf.taste.impl.recommender.knn.NonNegativeQuadraticOptimizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class RecommenderEvaluator {

    final static int NEIGHBORHOOD_NUM = 2;
    final static int RECOMMENDER_NUM = 3;

    public static void main(String[] args) throws TasteException, IOException {
    	double res=0;
    	String algr=null;
        String file = "datafile/job/pv.csv";
        RecommenderBuilder resBuilder = null;
        DataModel dataModel = RecommendFactory.buildDataModelNoPref(file);
        Map<String, Map<RecommenderBuilder, IRStatistics>>maps=new HashMap<String, Map<RecommenderBuilder,IRStatistics>>();
        Map<RecommenderBuilder, IRStatistics> remap=new HashMap<RecommenderBuilder, IRStatistics>();
        maps.put("userLoglikelihood", userLoglikelihood(dataModel).get("userLoglikelihood"));
        maps.put("userCityBlock", userCityBlock(dataModel).get("userCityBlock"));
        maps.put("userTanimoto", userTanimoto(dataModel).get("userTanimoto"));
        maps.put("itemLoglikelihood", itemLoglikelihood(dataModel).get("itemLoglikelihood"));
        maps.put("itemCityBlock", itemCityBlock(dataModel).get("itemCityBlock"));
        maps.put("itemTanimoto", itemTanimoto(dataModel).get("itemTanimoto"));
        maps.put("slopeOne", slopeOne(dataModel).get("slopeOne"));

        for (String string : maps.keySet()) {
			remap=maps.get(string);
			for (RecommenderBuilder recommenderBuilder : remap.keySet()) {
				IRStatistics stas=remap.get(recommenderBuilder);
				System.out.println(string+"======="+stas.getF1Measure());
				if(stas.getF1Measure()>res){
					algr=string;
					res=stas.getF1Measure();
					resBuilder=recommenderBuilder;
				}
			}
		}
        System.out.println("bestModel is:"+algr+"F1:"+res);
        
        LongPrimitiveIterator iter = dataModel.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            System.out.print(algr);
            RecommenderResult.result(uid, resBuilder, dataModel);
        }
    }
    
    public static Map<String,Map<RecommenderBuilder, IRStatistics>> userLoglikelihood(DataModel dataModel) throws TasteException, IOException {
        System.out.println("userLoglikelihood");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.LOGLIKELIHOOD, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST, userSimilarity, dataModel, NEIGHBORHOOD_NUM);
        RecommenderBuilder recommenderBuilder = RecommendFactory.userRecommender(userSimilarity, userNeighborhood, false);
        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas=	RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("userLoglikelihood", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> userCityBlock(DataModel dataModel) throws TasteException, IOException {
        System.out.println("userCityBlock");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.CITYBLOCK, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST, userSimilarity, dataModel, NEIGHBORHOOD_NUM);
        RecommenderBuilder recommenderBuilder = RecommendFactory.userRecommender(userSimilarity, userNeighborhood, false);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas=	RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("userCityBlock", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> userTanimoto(DataModel dataModel) throws TasteException, IOException {
        System.out.println("userTanimoto");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.TANIMOTO, dataModel);
        UserNeighborhood userNeighborhood = RecommendFactory.userNeighborhood(RecommendFactory.NEIGHBORHOOD.NEAREST, userSimilarity, dataModel, NEIGHBORHOOD_NUM);
        RecommenderBuilder recommenderBuilder = RecommendFactory.userRecommender(userSimilarity, userNeighborhood, false);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas= RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("userTanimoto", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> itemLoglikelihood(DataModel dataModel) throws TasteException, IOException {
        System.out.println("itemLoglikelihood");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.LOGLIKELIHOOD, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, false);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas=  RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("itemLoglikelihood", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> itemCityBlock(DataModel dataModel) throws TasteException, IOException {
        System.out.println("itemCityBlock");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.CITYBLOCK, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, false);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas= RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("itemCityBlock", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>>  itemTanimoto(DataModel dataModel) throws TasteException, IOException {
        System.out.println("itemTanimoto");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.TANIMOTO, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemRecommender(itemSimilarity, false);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas= RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("itemTanimoto", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> slopeOne(DataModel dataModel) throws TasteException, IOException {
        System.out.println("slopeOne");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        RecommenderBuilder recommenderBuilder = RecommendFactory.slopeOneRecommender();

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas=  RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("slopeOne", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> knnLoglikelihood(DataModel dataModel) throws TasteException, IOException {
        System.out.println("knnLoglikelihood");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.LOGLIKELIHOOD, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemKNNRecommender(itemSimilarity, new NonNegativeQuadraticOptimizer(), 10);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas=RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("knnLoglikelihood", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> knnTanimoto(DataModel dataModel) throws TasteException, IOException {
        System.out.println("knnTanimoto");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.TANIMOTO, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemKNNRecommender(itemSimilarity, new NonNegativeQuadraticOptimizer(), 10);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas=RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("knnTanimoto", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> knnCityBlock(DataModel dataModel) throws TasteException, IOException {
        System.out.println("knnCityBlock");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        ItemSimilarity itemSimilarity = RecommendFactory.itemSimilarity(RecommendFactory.SIMILARITY.CITYBLOCK, dataModel);
        RecommenderBuilder recommenderBuilder = RecommendFactory.itemKNNRecommender(itemSimilarity, new NonNegativeQuadraticOptimizer(), 10);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas= RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("knnCityBlock", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> svd(DataModel dataModel) throws TasteException {
        System.out.println("svd");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        RecommenderBuilder recommenderBuilder = RecommendFactory.svdRecommender(new ALSWRFactorizer(dataModel, 5, 0.05, 10));

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas=RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("svd", tmpRes);
        return res;
    }

    public static Map<String,Map<RecommenderBuilder, IRStatistics>> treeClusterLoglikelihood(DataModel dataModel) throws TasteException {
        System.out.println("treeClusterLoglikelihood");
        Map<String, Map<RecommenderBuilder, IRStatistics>>res=new HashMap<String,Map<RecommenderBuilder, IRStatistics>>();
        Map<RecommenderBuilder,IRStatistics> tmpRes=new HashMap<RecommenderBuilder, IRStatistics>();
        
        UserSimilarity userSimilarity = RecommendFactory.userSimilarity(RecommendFactory.SIMILARITY.LOGLIKELIHOOD, dataModel);
        ClusterSimilarity clusterSimilarity = RecommendFactory.clusterSimilarity(RecommendFactory.SIMILARITY.FARTHEST_NEIGHBOR_CLUSTER, userSimilarity);
        RecommenderBuilder recommenderBuilder = RecommendFactory.treeClusterRecommender(clusterSimilarity, 3);

        RecommendFactory.evaluate(RecommendFactory.EVALUATOR.AVERAGE_ABSOLUTE_DIFFERENCE, recommenderBuilder, null, dataModel, 0.7);
        IRStatistics stas=RecommendFactory.statsEvaluator(recommenderBuilder, null, dataModel, 2);
        tmpRes.put(recommenderBuilder,stas);
        res.put("treeClusterLoglikelihood", tmpRes);
        return res;
    }

}
