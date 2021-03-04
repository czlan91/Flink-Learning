/**
 * @author chenzhuanglan
 * @program flink-1.12.0
 * @description Kafka Server Demo
 *
 * 建议设置的项：
 * 1. 订阅的主题
 * 2. 反序列化规则
 * 3. 消费者属性-集群地址
 * 4. 消费者属性-消费者组id（如果不设置，会有默认的，但是默认的不方便管理）
 * 5. 消费者属性-offset重置规则，如earlieast/latest
 * 6. 动态分区检测（当kafka的分区数变化，增加时，Flink能够检测到！）
 * 7. 如果没有设置checkpoint，那么可以设置提交offset，后续学习了checkpoint会把offset随着做checkpoint的时候提交到checkpoint和默认主题中。
 * @date 2021/2/20 16:53
 */
public class kafkaServerDemo {

}
