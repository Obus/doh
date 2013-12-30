package doh.op.bk;

import com.synqera.bigkore.model.fact.Consumer;
import com.synqera.bigkore.model.shoppinglist.RankedProduct;
import com.synqera.bigkore.model.shoppinglist.RankedProductsGroup;
import com.synqera.bigkore.model.shoppinglist.RankedShoppingList;
import com.synqera.bigkore.rank.OptionsBuilder;
import com.synqera.bigkore.rank.PlatformUtils;
import com.synqera.bigkore.rank.phases.Phase;
import com.synqera.bigkore.rank.phases.ShopListPhase;
import com.synqera.bigkore.rank.shoplist.AvgTopKRankedFilter;
import com.synqera.bigkore.rank.shoplist.RankedFilter;
import com.synqera.bigkore.rank.shoplist.ShopListTargetGroupReducer;
import com.synqera.bigkore.rank.shoplist.TopKRankedFilter;
import com.synqera.hadoop.io.MapFile;
import doh.api.ds.KVDataSet;
import doh.api.ds.KVDataSetFactory;
import doh.ds.MapKVDataSet;
import doh.op.Context;
import doh.op.TempPathManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DOHShopListPhase  extends Phase {
    private final static Logger LOGGER = LoggerFactory.getLogger(ShopListPhase.class);

    private Path[] totalRetailSubPaths;
    private Path recommenderToHBase;

    private TempPathManager tempPathManager;
    private Context context;
    private boolean useConditions;

    private RankedFilter<RankedProduct> rankedProductsFilter;
    private RankedFilter<RankedProductsGroup> rankedProductsGroupsFilter;

    @Override
    protected int doRun() throws Exception {
        Configuration conf = getConf();
        HadoopUtil.delete(conf, getTempPath());



        final MapKVDataSet<String, Integer>
                productRestMap = initProductRestMap();
        KVDataSet<ImmutableBytesWritable, KeyValue>
                recommenderHBase = KVDataSetFactory.create(context, recommenderToHBase);
        KVDataSet<BytesWritable, String>
                totalRetail = KVDataSetFactory.create(context, totalRetailSubPaths);


        if (productRestMap != null) {
            KVDataSet<Consumer, ObjectWritable>
                    consumerConditionedBaskets = totalRetail.
                    flatMap(new ShopListOps.RawUSToConsumerConditionProductOp(useConditions)).
                    reduce(new ShopListOps.ConsumersConditionedProductsReduceOp()).
                    map(new ShopListOps.ConsumersConditionedProductsGroupsMapOp()).
                    map(new ShopListOps.ConditionedRPGFilterByRestOp(productRestMap)).
                    map(new ShopListOps.ConditionedRPGFilterByRankOp(rankedProductsFilter, rankedProductsGroupsFilter)).
                    map(new ShopListOps.ToConsumerBasketOp());

            KVDataSet<Consumer, ObjectWritable>
                    consumerOffers = recommenderHBase.
                    map(new ShopListOps.RecommenderHBaseResultsParseOp()).
                    reduce(new ShopListOps.RecommendationsToOffersOp(productRestMap));

            KVDataSet<Consumer, ObjectWritable>
                    consumerOffersAndConditionedBaskets = consumerConditionedBaskets.
                    comeTogetherRightNow(consumerOffers);

            KVDataSet<Consumer, RankedShoppingList>
                    consumerRankedShoppingList =  consumerOffersAndConditionedBaskets.
                    reduce(new ShopListOps.OffersBasketsToShoppingList());

            consumerRankedShoppingList.beReady();
        }

        else {
            throw new UnsupportedOperationException();
        }

        return 0;
    }

    private void readFilters(Configuration conf) {
        String mode = conf.get("rank.shopList.mode");
        if (mode.equals(ShopListTargetGroupReducer.TOP_K)) {
            int topK = Integer.parseInt(conf.get("rank.shopList.mode.topK"));
            rankedProductsGroupsFilter = new TopKRankedFilter<RankedProductsGroup>(topK);
        }
        else if (mode.equals(ShopListTargetGroupReducer.AVG_TOP_K)) {
            int topK = Integer.parseInt(conf.get("rank.shopList.mode.avgTopK"));
            rankedProductsGroupsFilter = new AvgTopKRankedFilter<RankedProductsGroup>(topK);
        }
        else {
            throw new IllegalArgumentException("Unknown RankedProductsGroup filter type: " + mode);
        }
        int productsInGroup = Integer.parseInt(conf.get("rank.shopList.productsInGroup"));
        rankedProductsFilter = new AvgTopKRankedFilter<RankedProduct>(productsInGroup);
    }

    public MapKVDataSet<String, Integer> initProductRestMap() throws Exception {
//        String shop = getConf().get("rank.shopList.shopName");
//        shop  = "land8";
//        if (shop == null) {
//            return null;
//        }
//        Path productRestMap = getTempPath("product-rest.map");
//        int res = runPhase(new LandFTPRestXMLToProductRestMapPhase(), OptionsBuilder.options()
//                .add("output", productRestMap)
//                .add("shop", shop)
//                .make());
//        if (res != 0) {
//            LOGGER.warn("Land product-rest.map creation phase failed, go further without it.");
//            return null;
//        }
        Path productRestMap = getTempPath("product-rest.map");
        Path data = new Path(productRestMap, MapFile.DATA_FILE_NAME);
        SequenceFile.Writer writer = SequenceFile.createWriter(data.getFileSystem(getConf()), getConf(), data, Text.class, IntWritable.class);
        for (int i = 0; i < 5000; ++i) {
            if (i % 10 != 0) {
                writer.append(new Text(Integer.toString(i)), new IntWritable(i));
            }
        }
        writer.close();

        MapKVDataSet<String, Integer> map = KVDataSetFactory.
                createMap(context, new Path(productRestMap, MapFile.DATA_FILE_NAME));
        return map;
    }

    @Override
    protected void addOptions() {
        addOption("totalRetail", null, "path to total-retail");
        // addOption("toHBase", null, "path to toHBase folder");
        addOutputOption();
        addOption("recToHBase", null, "Recommender toHBase folder path");
    }

    @Override
    protected int readOptions() {
        Path totalRetailPath = new Path(getOption("totalRetail"));
        Configuration conf = getConf();
        try {
            if (!totalRetailPath.getFileSystem(conf).exists(totalRetailPath)) {
                LOGGER.warn("Retail path {} doesn't exists. Exit", totalRetailPath);
                return -1;
            }
            totalRetailSubPaths = PlatformUtils.listDirectories(conf, totalRetailPath);
            if (totalRetailSubPaths.length == 0) {
                LOGGER.warn("Zero retail data {}. Exit", totalRetailSubPaths);
                return -1;
            }
        } catch (IOException e) {
            LOGGER.error("Error while extracting total retail sub paths", e);
            return -1;
        }
        // toHBasePath = new Path(getOption("toHBase"));
        recommenderToHBase = new Path(getOption("recToHBase"));

        tempPathManager = new TempPathManager(getTempPath("temp"));
        List<Pair<Class, Object>> typeAdapters = new ArrayList<Pair<Class, Object>>(1);
        typeAdapters.add(new Pair<Class, Object>(RankedFilter.class, new GsonSerDe.RankedFilterGsonSerDe()));
        context = Context.create(getConf(), tempPathManager, typeAdapters);
        useConditions = conf.getBoolean("rank.shopList.basket.useConditions", false);
        readFilters(conf);
        return 0;
    }

    @Override
    public String getName() {
        return "DOHshopList";
    }


    public static void main(String [] args) throws Exception {
        String prefix = "/Users/senov/CODE/git/bigkore/";
        Path toHBase = new Path(prefix + "temp/toHBase/rank/rec/");
        Path totalRetail = new Path(prefix + "../../bk_output/total-retail/");
        Path output = new Path("temp/DOHShopList/output");
        Path tempDir = new Path("temp/DOHShopList/tempDir");
        Phase phase = new DOHShopListPhase();

        Configuration conf = new Configuration();
        conf.addResource("rank-default.xml");
        conf.addResource("rank-site.xml");


        phase.setConf(conf);
        phase.setCommonContext(new PhaseCommonContext());
        ToolRunner.run(conf, phase, OptionsBuilder.options()
                .add("totalRetail", totalRetail)
                .add("recToHBase", toHBase)
                .add("output", output)
                .add("tempDir", tempDir)
                .make());
        //phase.setIndividualContext(new PhaseIndividualContext(new AtomicInteger(0), phase.parseArguments(OptionsBuilder.)));
    }
}
