package org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.config;

import java.util.Map;

/**
 * Description Copyright © 启明星辰 版权所有
 *
 * @author chaorongzhi
 * @date 2023/12/8
 */
public class HwEsAuthConfig {
    private int connectTimeout;
    private int socketTimeout;
    private int connectionRequestTimeout;
    private int maxConnPerRoute;
    private int maxConnTotal;
    private String isSecureMode;
    private String principal;
    private String snifferEnable;
    private String customJaasPath;
    private String krb5Path;
    private String keytab;

    private static final String CONNECT_TIMEOUT = "connectTimeout";
    private static final String SOCKET_TIMOUT = "socketTimeout";
    private static final String CONNECTION_REQUEST_TIMOUT = "connectionRequestTimeout";
    private static final String MAX_CONN_PER_ROUTE = "maxConnPerRoute";
    private static final String MAX_CONN_TOTAL = "maxConnTotal";
    private static final String IS_SECURE_MODE = "isSecureMode";
    private static final String PRINCIPAL = "principal";
    private static final String SNIFFER_ENABLE = "snifferEnable";
    private static final String CUSTOM_JASS_PATH = "customJaasPath";
    private static final String KRB5_PATH = "krb5Path";
    private static final String KEYTAB = "keytab";

    public HwEsAuthConfig(Map<String, String> config) {
        if (config.containsKey(CONNECT_TIMEOUT)) {
            this.connectTimeout = Integer.parseInt(config.get(CONNECT_TIMEOUT));
        }
        if (config.containsKey(SOCKET_TIMOUT)) {
            this.socketTimeout = Integer.parseInt(config.get(SOCKET_TIMOUT));
        }
        if (config.containsKey(CONNECTION_REQUEST_TIMOUT)) {
            this.connectionRequestTimeout = Integer.parseInt(config.get(CONNECTION_REQUEST_TIMOUT));
        }
        if (config.containsKey(MAX_CONN_PER_ROUTE)) {
            this.maxConnPerRoute = Integer.parseInt(config.get(MAX_CONN_PER_ROUTE));
        }
        if (config.containsKey(MAX_CONN_TOTAL)) {
            this.maxConnTotal = Integer.parseInt(config.get(MAX_CONN_TOTAL));
        }
        if (config.containsKey(IS_SECURE_MODE)) {
            this.isSecureMode = config.get(IS_SECURE_MODE);
        }
        if (config.containsKey(PRINCIPAL)) {
            this.principal = config.get(PRINCIPAL);
        }
        if (config.containsKey(SNIFFER_ENABLE)) {
            this.snifferEnable = config.get(SNIFFER_ENABLE);
        }
        if (config.containsKey(CUSTOM_JASS_PATH)) {
            this.customJaasPath = config.get(CUSTOM_JASS_PATH);
        }
        if (config.containsKey(KRB5_PATH)) {
            this.krb5Path = config.get(KRB5_PATH);
        }
        if (config.containsKey(KEYTAB)) {
            this.keytab = config.get(KEYTAB);
        }
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    public int getMaxConnPerRoute() {
        return maxConnPerRoute;
    }

    public int getMaxConnTotal() {
        return maxConnTotal;
    }

    public String getIsSecureMode() {
        return isSecureMode;
    }

    public String getPrincipal() {
        return principal;
    }

    public String getSnifferEnable() {
        return snifferEnable;
    }

    public String getCustomJaasPath() {
        return customJaasPath;
    }

    public String getKrb5Path() {
        return krb5Path;
    }

    public String getKeytab() {
        return keytab;
    }
}
