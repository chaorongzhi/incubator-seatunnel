package org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

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

    public HwEsAuthConfig(Config config) {
        if (config.hasPath(CONNECT_TIMEOUT)) {
            this.connectTimeout = config.getInt(CONNECT_TIMEOUT);
        }
        if (config.hasPath(SOCKET_TIMOUT)) {
            this.socketTimeout = config.getInt(SOCKET_TIMOUT);
        }
        if (config.hasPath(CONNECTION_REQUEST_TIMOUT)) {
            this.connectionRequestTimeout = config.getInt(CONNECTION_REQUEST_TIMOUT);
        }
        if (config.hasPath(MAX_CONN_PER_ROUTE)) {
            this.maxConnPerRoute = config.getInt(MAX_CONN_PER_ROUTE);
        }
        if (config.hasPath(MAX_CONN_TOTAL)) {
            this.maxConnTotal = config.getInt(MAX_CONN_TOTAL);
        }
        if (config.hasPath(IS_SECURE_MODE)) {
            this.isSecureMode = config.getString(IS_SECURE_MODE);
        }
        if (config.hasPath(PRINCIPAL)) {
            this.principal = config.getString(PRINCIPAL);
        }
        if (config.hasPath(SNIFFER_ENABLE)) {
            this.snifferEnable = config.getString(SNIFFER_ENABLE);
        }
        if (config.hasPath(CUSTOM_JASS_PATH)) {
            this.customJaasPath = config.getString(CUSTOM_JASS_PATH);
        }
        if (config.hasPath(KRB5_PATH)) {
            this.krb5Path = config.getString(KRB5_PATH);
        }
        if (config.hasPath(KEYTAB)) {
            this.keytab = config.getString(KEYTAB);
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
