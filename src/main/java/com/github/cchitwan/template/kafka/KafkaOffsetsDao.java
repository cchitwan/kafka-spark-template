package com.github.cchitwan.template.kafka;

import com.github.cchitwan.template.config.IKafkaOffsetManagementConfig;
import com.github.cchitwan.template.utils.Constants;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author chanchal.chitwan on 06/04/17.
 */
public class KafkaOffsetsDao implements Serializable{

    private transient static CuratorFramework zk;
    private String zkParentPath;
    private static IKafkaOffsetManagementConfig config;
    private Long myOffsetRange;

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        createZk();
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        if(zk != null) {
            zk.close();
            System.out.println("Closed zookeeper connection");
        }
    }

    public KafkaOffsetsDao(IKafkaOffsetManagementConfig config, String appName, Long myOffsetRange) {
        this.config = config;
        this.myOffsetRange = myOffsetRange;
        zkParentPath = "/"+appName+"/" + config.getConsumerGroup();
        System.out.println("initializing zk ....");
        createZk();
    }

    private static void createZk() {
        System.out.println("config for zk : "+config);
        zk = CuratorFrameworkFactory.newClient(config.getZookeeperConnectionString(), new RetryForever(500));
        zk.start();
        System.out.println("Created zookeeper connection");
//        return zk;
    }

    private void checkZk(){
        if(zk == null ){
            System.out.println("checkZk() called");
            createZk();
        }
    }

    public Map<Integer, OffsetRange> fetchStoredOffsets(String kafkaTopic) throws KafkaOffsetsDaoException {
        try {
            Map<Integer, OffsetRange> retVal = new HashMap<>();

            List<String> children;
            try {
                checkZk();
                children = zk.getChildren().forPath((zkParentPath + "/" + kafkaTopic));
            } catch (KeeperException.NoNodeException e) {
                System.out.println("No Node exist: ");
                final Map<Integer, Pair<Long, Long>> latestOffsets = getLatestOffsets(kafkaTopic);
                //latestOffsets.entrySet().stream().forEach(ep->System.out.println("OffSet Before "+ep.getKey()+" "+ep.getValue().toString()));
                Map<Integer, Pair<Long, Long>> pairMap = latestOffsets.entrySet().stream().map(ev -> {
                    Pair temp = ev.getValue();
                    Long left = (Long) temp.getRight() - myOffsetRange;
                    Long right = (Long) temp.getRight();
                    ev.setValue(ImmutablePair.of(left, right));
                    return ev;
                }).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
                ArrayList<OffsetRange> offsetRangesList = new ArrayList<>(pairMap.size());
                retVal = getOffsetRangeFromPair(kafkaTopic, pairMap );
                //pairMap.entrySet().stream().forEach(ep->System.out.println("OffSet After "+ep.getKey()+" "+ep.getValue().toString()));
                retVal.entrySet().stream().forEach(ep->System.out.println("partition: "+ep.getKey()+" offset range: "+ep.getValue().toString()));
                System.out.println("offsetRangesList: "+offsetRangesList);

                return retVal;
            }

            for (String ch : children) {
                String path = zkParentPath + "/" + kafkaTopic + "/" + ch;
                byte[] b = zk.getData().forPath(path);
                com.github.cchitwan.template.kafka.OffsetRange newOr = Constants.objectMapper.readValue(b, com.github.cchitwan.template.kafka.OffsetRange.class);
                org.apache.spark.streaming.kafka.OffsetRange or = com.github.cchitwan.template.kafka.OffsetRange.toOffsetRange(newOr);
                retVal.put(Integer.parseInt(ch), or);
            }

            return retVal;
        } catch (Exception e) {
            System.out.println("fetchStoredOffsets failed with " + e);
            throw new KafkaOffsetsDaoException(e);
        }
    }

    /**
     *
     * @param or
     * @param kafkaTopic
     * @throws KafkaOffsetsDaoException
     * path example - /<appName>/<kafka-consumer-group>/<topic>/<partition>
     */
    public void storeOffsets(OffsetRange or, String kafkaTopic) throws KafkaOffsetsDaoException {
        try {
            checkZk();
            String path = zkParentPath + "/" + kafkaTopic + "/" + or.partition();
            Stat stat = zk.checkExists().forPath(path);
            if (stat == null) {
                zk.create().creatingParentsIfNeeded().forPath(path);
            }

            com.github.cchitwan.template.kafka.OffsetRange newOr = com.github.cchitwan.template.kafka.OffsetRange.fromOffsetRange(or);
            zk.setData().forPath(path, Constants.objectMapper.writeValueAsBytes(newOr));
        } catch (Exception e) {
            throw new KafkaOffsetsDaoException(e);
        }
    }

    public void storeOffsets(HasOffsetRanges rdd) throws KafkaOffsetsDaoException {
        try {
            checkZk();
            OffsetRange[] offsetRanges = rdd.offsetRanges();
            for (OffsetRange or : offsetRanges) {
                String path = zkParentPath + "/" + or.topic() + "/" + or.partition();
                Stat stat = zk.checkExists().forPath(path);
                if (stat == null) {
                    zk.create().creatingParentsIfNeeded().forPath(path);
                }
                com.github.cchitwan.template.kafka.OffsetRange newOr = com.github.cchitwan.template.kafka.OffsetRange.fromOffsetRange(or);

                zk.setData().forPath(path, Constants.objectMapper.writeValueAsBytes(newOr));
            }
        } catch (Exception e) {
            throw new KafkaOffsetsDaoException(e);
        }
    }

    public Map<Integer, Pair<Long, Long>> getLatestOffsets(String kafkaTopic) throws KafkaOffsetsDaoException {
        try {
            ZkClient zkClient = new ZkClient(config.getZookeeperConnectionString(),
                                             config.getZookeeperConnectTimeout(), config.getZookeeperConnectTimeout(),
                                             ZKStringSerializer$.MODULE$);
            // get all partitions, some dance with scala collections!
            Buffer topics = JavaConverters.asScalaBufferConverter(Collections.singletonList(kafkaTopic)).asScala();
            Tuple2 t2 = (Tuple2) ZkUtils.getPartitionsForTopics(zkClient, topics).head();
            Collection<Object> partitions = JavaConverters.seqAsJavaListConverter((Seq<Object>) t2._2()).asJava();

            Map<Integer, Pair<Long, Long>> retVal = new HashMap<>();
            for (Object partition : partitions) {
                Integer partitionInt = (Integer) partition;
                SimpleConsumer consumer = createSimpleConsumer(zkClient, kafkaTopic, partitionInt);
                if (consumer == null) {
                    System.out.println("Couldn't create consumer for topic: " +  kafkaTopic + " partition: " + partitionInt);
                    continue;
                }

                TopicAndPartition topicAndPartition = new TopicAndPartition(kafkaTopic, partitionInt);

                Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestReqtInfo =
                        Collections.singletonMap(topicAndPartition,
                                new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
                OffsetRequest earliestReq = new OffsetRequest(earliestReqtInfo, kafka.api.OffsetRequest.CurrentVersion(), kafka.api.OffsetRequest.DefaultClientId());

                Map<TopicAndPartition, PartitionOffsetRequestInfo> latestReqtInfo =
                        Collections.singletonMap(topicAndPartition,
                                new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
                OffsetRequest latestReq = new OffsetRequest(latestReqtInfo, kafka.api.OffsetRequest.CurrentVersion(), kafka.api.OffsetRequest.DefaultClientId());

                OffsetResponse earliestResp = consumer.getOffsetsBefore(earliestReq);
                OffsetResponse latestResp = consumer.getOffsetsBefore(latestReq);
                if (earliestResp.hasError()) {
                    System.out.println("Error fetching earliest offset from broker. Reason: " + earliestResp.errorCode(kafkaTopic, partitionInt));
                    continue;
                }
                if (latestResp.hasError()) {
                    System.out.println("Error fetching earliest offset from broker. Reason: " + latestResp.errorCode(kafkaTopic, partitionInt));
                    continue;
                }
                consumer.close();
                long ealiestOffset = earliestResp.offsets(kafkaTopic, partitionInt)[0];
                long latestOffset = latestResp.offsets(kafkaTopic, partitionInt)[0];
                retVal.put(partitionInt, new ImmutablePair<>(ealiestOffset, latestOffset));
            }

            return retVal;
        } catch (Exception e) {
            throw new KafkaOffsetsDaoException(e);
        }
    }

    public OffsetRange[] getOffsetRangeForRead(String kafkaTopic) throws KafkaOffsetsDaoException {
        try {
            Map<Integer, OffsetRange> storedOffsets = fetchStoredOffsets(kafkaTopic);
            Map<Integer, Pair<Long, Long>> latestOffsets = getLatestOffsets(kafkaTopic);
            List<OffsetRange> ors = new ArrayList<>(latestOffsets.size());
            latestOffsets.forEach(((partition, offsets) -> {
                long begningOffset;
                if (storedOffsets.containsKey(partition)) {
                    begningOffset = storedOffsets.get(partition).untilOffset();
                } else {
                    begningOffset = offsets.getLeft();
                }

                OffsetRange or = OffsetRange.apply(kafkaTopic, partition, begningOffset, offsets.getRight());
                ors.add(or);
            }));
            OffsetRange[] offsetRanges = new OffsetRange[ors.size()];
            ors.toArray(offsetRanges);

            return offsetRanges;
        } catch (Exception e) {
            throw new KafkaOffsetsDaoException(e);
        }
    }

    private Map<Integer, OffsetRange> getOffsetRangeFromPair(final String kafkaTopic, final Map<Integer, Pair<Long, Long>> latestOffsets) {
        final Map<Integer, OffsetRange> storedOffsets = new HashMap<>();
        latestOffsets.forEach(((partition, offsets) -> {
            long begningOffset;
            if (storedOffsets.containsKey(partition)) {
                begningOffset = storedOffsets.get(partition).untilOffset();
            } else {
                begningOffset = offsets.getLeft();
            }

            OffsetRange or = OffsetRange.apply(kafkaTopic, partition, begningOffset, offsets.getRight());
            storedOffsets.put(partition, or);
        }));
        return storedOffsets;
    }

    private SimpleConsumer createSimpleConsumer(ZkClient zkClient, String topic, int partition) throws KafkaOffsetsDaoException {
        try {
            String defaultClientId = "";
            Option<Object> bid = ZkUtils.getLeaderForPartition(zkClient, topic, partition);
            if (bid.isEmpty()) {
                System.out.println("Cannot find leader for topic: " + topic + " partition: " + partition);
                return null;
            }
            Broker brokerInfo = ZkUtils.getBrokerInfo(zkClient, (Integer) bid.get()).get();
            SimpleConsumer simpleConsumer = new SimpleConsumer(brokerInfo.host(), brokerInfo.port(), 30000, 64 * 1024, defaultClientId);
            return simpleConsumer;
        } catch (Exception e) {
            throw new KafkaOffsetsDaoException(e);
        }
    }
}
