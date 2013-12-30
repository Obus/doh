package doh.op.bk;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.synqera.bigkore.model.UserStory;
import com.synqera.bigkore.model.fact.Consumer;
import com.synqera.bigkore.model.fact.Fact;
import com.synqera.bigkore.model.fact.Product;
import com.synqera.bigkore.model.fact.Time;
import com.synqera.bigkore.model.fact.derivatives.DayOfWeek;
import com.synqera.bigkore.model.fact.derivatives.TimeOfDay;
import com.synqera.bigkore.model.shoppinglist.RankedBasket;
import com.synqera.bigkore.model.shoppinglist.RankedBaskets;
import com.synqera.bigkore.model.shoppinglist.RankedOffers;
import com.synqera.bigkore.model.shoppinglist.RankedProduct;
import com.synqera.bigkore.model.shoppinglist.RankedProductsGroup;
import com.synqera.bigkore.model.shoppinglist.RankedShoppingList;
import com.synqera.bigkore.rank.shoplist.RankedFilter;
import doh.api.OpParameter;
import doh.api.op.FlatMapOp;
import doh.api.op.KV;
import doh.api.op.MapOp;
import doh.api.op.ReduceOp;
import doh.ds.MapKVDataSet;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.ObjectWritable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShopListOps {
    public static class RawUSToConsumerConditionProductOp extends FlatMapOp<BytesWritable, String, String, Product> {
        private transient static final UserStory us = new UserStory();

        @OpParameter
        private boolean useConditions;

        public RawUSToConsumerConditionProductOp() {}

        public RawUSToConsumerConditionProductOp(boolean useConditions) {
            this.useConditions = useConditions;
        }

        @Override
        public void flatMap(BytesWritable bytesWritable, String usString) {
            us.fromString(usString);
            Time time = us.time();

            DayOfWeek dow = DayOfWeek.fromTime(time);
            TimeOfDay tod = TimeOfDay.fromTime(time);

            final String c = us.consumer().toString(),
                    c_dow = c + "," + dow,
                    c_tod = c + "," + tod,
                    c_dow_tod = c_dow + "," + tod;

            for (Product product : us.products()) {
                emitKeyValue(c, product);
                if (useConditions) {
                    emitKeyValue(c_dow, product);
                    emitKeyValue(c_tod, product);
                    emitKeyValue(c_dow_tod, product);
                }
            }
        }
    }

    public static class ConsumersConditionedProductsReduceOp
            extends ReduceOp<String, Product, Consumer, ConditionedPurchases> {
        @Override
        public KV<Consumer, ConditionedPurchases> reduce(String consumerCondition, Iterable<Product> products) {
            List<Fact> conditions = new ArrayList<Fact>();
            String[] splits = consumerCondition.split(",");
            Consumer consumer = (Consumer) Fact.parseString(splits[0]);
            for (int i = 1; i < splits.length; ++i) {
                conditions.add(Fact.parseString(splits[i]));
            }

            Map<Product, Integer> freqMap = countFrequencies(products);
            return keyValue(consumer, new ConditionedPurchases(conditions, freqMap));
        }
    }

    public static class ConsumersConditionedProductsGroupsMapOp
            extends MapOp<Consumer, ConditionedPurchases, Consumer, ConditionedRPGs> {
        @Override
        public KV<Consumer, ConditionedRPGs> map(Consumer consumer, ConditionedPurchases conditionedPurchases) {
            ConditionedRPGs conditionedRPGs = new ConditionedRPGs(
                    conditionedPurchases.conditions,
                    toRankedProductsGroups(conditionedPurchases.productFreqMap)
            );
            return keyValue(consumer, conditionedRPGs);
        }
    }

    public static class ConditionedRPGFilterByRestOp extends MapOp<Consumer, ConditionedRPGs, Consumer, ConditionedRPGs> {
        @OpParameter
        MapKVDataSet<String, Integer> productRestMap;

        public ConditionedRPGFilterByRestOp() {}
        public ConditionedRPGFilterByRestOp(MapKVDataSet<String, Integer> productRestMap) {
            this.productRestMap = productRestMap;
        }

        @Override
        public KV<Consumer, ConditionedRPGs> map(Consumer consumer, ConditionedRPGs conditionedRPGs) {
            ConditionedRPGs filteredByRestConditionedRPGs = new ConditionedRPGs(
                    conditionedRPGs.conditions,
                    filter(conditionedRPGs.rpgList, new Predicate<RankedProductsGroup>() {
                        @Override
                        public boolean apply(@Nullable RankedProductsGroup input) {
                            input.rankedProducts = filter(input.rankedProducts, new Predicate<RankedProduct>() {
                                @Override
                                public boolean apply(@Nullable RankedProduct input) {
                                    Integer rest = productRestMap.get(input.productId);
                                    return rest != null && rest > 0;
                                }
                            });
                            return input.rankedProducts.size() > 0;
                        }
                    })
            );
            return keyValue(consumer, filteredByRestConditionedRPGs);
        }
    }

    public static class ConditionedRPGFilterByRankOp extends MapOp<Consumer, ConditionedRPGs, Consumer, ConditionedRPGs> {
        @OpParameter
        private RankedFilter<RankedProduct> rankedProductsFilter;
        @OpParameter
        private RankedFilter<RankedProductsGroup> rankedProductsGroupsFilter;

        public ConditionedRPGFilterByRankOp(RankedFilter<RankedProduct> rankedProductsFilter, RankedFilter<RankedProductsGroup> rankedProductsGroupsFilter) {
            this.rankedProductsFilter = rankedProductsFilter;
            this.rankedProductsGroupsFilter = rankedProductsGroupsFilter;
        }
        public ConditionedRPGFilterByRankOp() {}

        @Override
        public KV<Consumer, ConditionedRPGs> map(Consumer consumer, ConditionedRPGs conditionedRPGs) {
            List<RankedProductsGroup> filteredRpgList = rankedProductsGroupsFilter.filter(conditionedRPGs.rpgList);
            for (RankedProductsGroup rpg : filteredRpgList) {
                rpg.rankedProducts = rankedProductsFilter.filter(rpg.rankedProducts);
            }
            ConditionedRPGs filteredByRestConditionedRPGs = new ConditionedRPGs(
                    conditionedRPGs.conditions,
                    filteredRpgList
            );
            return keyValue(consumer, filteredByRestConditionedRPGs);
        }
    }

    public static class ToConsumerBasketOp extends MapOp<Consumer, ConditionedRPGs, Consumer, ObjectWritable> {
        private transient static final ObjectWritable ow = new ObjectWritable();
        @Override
        public KV<Consumer, ObjectWritable> map(Consumer consumer, ConditionedRPGs conditionedRPGs) {
            RankedBasket basket = new RankedBasket();
            basket.conditions = conditionedRPGs.conditions;
            basket.groupsList = conditionedRPGs.rpgList;
            ow.set(basket);
            return keyValue(consumer, ow);
        }
    }

    public static class ConditionedPurchases {
        public final List<Fact> conditions;
        public final Map<Product, Integer> productFreqMap;

        public ConditionedPurchases(List<Fact> conditions, Map<Product, Integer> productFreqMap) {
            this.conditions = conditions;
            this.productFreqMap = productFreqMap;
        }
    }

    public static class ConditionedRPGs {
        public final List<Fact> conditions;
        public final List<RankedProductsGroup> rpgList;

        public ConditionedRPGs(List<Fact> conditions, List<RankedProductsGroup> rpgList) {
            this.conditions = conditions;
            this.rpgList = rpgList;
        }
    }



    public static class RecommenderHBaseResultsParseOp extends MapOp<ImmutableBytesWritable, KeyValue, Consumer, RankedProduct> {
        private static final RankedProduct rankedProduct = new RankedProduct();

        @Override
        public KV<Consumer, RankedProduct> map(ImmutableBytesWritable key, KeyValue value) {
            Consumer client = (Consumer) Fact.parseString(Bytes.toString(value.getRow()));
            Product product = (Product) Fact.parseString(Bytes.toString(value.getQualifier()));
            Double rank = Double.parseDouble(Bytes.toString(value.getValue()));

            rankedProduct.productId = product.getId();
            rankedProduct.rank = rank;
            return keyValue(client, rankedProduct);
        }
    }

    public static class RecommendationsToOffersOp extends ReduceOp<Consumer, RankedProduct, Consumer, ObjectWritable> {
        private static final List<RankedProduct> rpList = new ArrayList<RankedProduct>();
        private static final RankedOffers offers = new RankedOffers();
        private static final ObjectWritable objectWritable = new ObjectWritable();
        @OpParameter
        MapKVDataSet<String, Integer> productRestMap;

        public RecommendationsToOffersOp() {}
        public RecommendationsToOffersOp(MapKVDataSet<String, Integer> productRestMap) {
            this.productRestMap = productRestMap;
        }

        @Override
        public KV<Consumer, ObjectWritable> reduce(Consumer consumer, Iterable<RankedProduct> rankedProducts) {
            rpList.clear();

            for (RankedProduct rp : rankedProducts) {
                Integer rest = productRestMap.get(rp.productId);
                if (rest != null && rest > 0) {
                    rpList.add(rp.copy());
                }
            }

            offers.rankedProducts = rpList;
            objectWritable.set(offers);
            return keyValue(consumer, objectWritable);
        }
    }



    public static class OffersBasketsToShoppingList extends ReduceOp<Consumer, ObjectWritable, Consumer, RankedShoppingList> {
        private static final List<RankedBasket> basketList = Lists.newArrayList();
        private static final RankedShoppingList shoppingList = new RankedShoppingList();


        @Override
        public KV<Consumer, RankedShoppingList> reduce(Consumer consumer, Iterable<ObjectWritable> objectWritables) {
            basketList.clear();
            RankedOffers offers = null;
            for (ObjectWritable ow : objectWritables) {
                Object o = ow.get();
                if (o instanceof RankedBasket) {
                    basketList.add(((RankedBasket) o).shallowCopy());
                }
                else if (o instanceof RankedOffers) {
                    if (offers != null) {
                        throw new IllegalArgumentException("More than one offer for consumer: " + consumer);
                    }
                    offers = (RankedOffers) o;
                }
            }
            if (offers == null) {
                offers = new RankedOffers();
                offers.rankedProducts = new ArrayList<RankedProduct>();
            }
            RankedBaskets baskets = new RankedBaskets();
            baskets.basketList = basketList;
            shoppingList.offers = offers;
            shoppingList.baskets = baskets;
            shoppingList.consumerId = consumer.getId();
            return keyValue(consumer, shoppingList);
        }

    }








    public static <T> List<T> filter(Collection<T> collection, Predicate<? super T> predicate) {
        return Lists.newArrayList(Collections2.filter(collection, predicate));
    }


    public static List<RankedProductsGroup> toRankedProductsGroups(Map<Product, Integer> productFreqMap) {
        Map<String, RankedProductsGroup> rpgMap = new HashMap<String, RankedProductsGroup>();
        for (Map.Entry<Product, Integer> pf : productFreqMap.entrySet()) {
            Product p = pf.getKey();
            RankedProductsGroup rpg = rpgMap.get(p.getGroup());
            if (rpg == null) {
                rpg = new RankedProductsGroup();
                rpg.id = p.getGroup();
                rpg.rankedProducts = new ArrayList<RankedProduct>();
                rpgMap.put(p.getGroup(), rpg);
            }
            rpg.rankedProducts.add(new RankedProduct(p.getId(), pf.getValue()));
        }
        List<RankedProductsGroup> rpgList = new ArrayList<RankedProductsGroup>(rpgMap.values());
        for (RankedProductsGroup rpg : rpgList) {
            rpg.rank = 0;
            for (RankedProduct rp : rpg.rankedProducts) {
                rpg.rank += rp.rank;
            }
        }
        return rpgList;
    }



    public static Map<Product, Integer> countFrequencies(Iterable<Product> values) {
        Map<Product, Integer> freqMap = new HashMap<Product, Integer>();
        for (Product p : values) {
            Product pIdOnly = (Product) p.clone();
            Integer freq = freqMap.get(pIdOnly);
            freq = (freq == null) ? 1 : freq + 1;
            freqMap.put(pIdOnly, freq);
        }
        return freqMap;
    }

}
