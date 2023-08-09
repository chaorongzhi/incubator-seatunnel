package org.apache.seatunnel.connectors.seatunnel.kafka.hw.utils;
/**
 * @author patrick Created on 2022/6/7 15:51
 * @description kafka sasl认证常量
 */
public class KafkaSASLConstants {

    /** 协议 */
    public static String protocol = "SASL_PLAINTEXT";
    /** 认证途径 */
    public static String saslMechanism = "PLAIN";
    /** 认证配置 */
    public static String saslJaasConfig =
            "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                    + "        username=\"kafka\"\n"
                    + "        password=\"m33dt0lb6uAGR+4zgpR7jsR8YMIkXhQ=\";";

    /** huawei配置:krb5.conf路径 */
    public static String krb5Config = "";

    /** huawei配置：ZookeeperServerPrincipal */
    public static String zookeeperServerPrincipal = "zookeeper/hadoop.hadoop.com";

    /** kafka连接地址 */
    public static String bootstrapServers = "";

    /** kafka KERBEROS_DOMAIN_NAME 域名 */
    public static String kerberosDomainName = "hadoop.hadoop.com";

    /** kafka KERBEROS_SERVER_NAME */
    public static String kerberosServerName = "kafka";
}
