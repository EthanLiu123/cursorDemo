# Flink集群配置
FLINK_CLUSTER_CONFIG = {
    'jobmanager': 'hadoop105:8081',
    'rest_api': 'http://hadoop105:8081',
    'parallelism': 1,
    'memory': {
        'taskmanager': '1024m',
        'jobmanager': '1024m'
    },
    'security': {
        'enabled': False,
        'username': None,
        'password': None,
        'kerberos': {
            'enabled': False,
            'keytab': None,
            'principal': None
        }
    },
    'high_availability': {
        'enabled': False,
        'zookeeper_quorum': None,
        'zookeeper_root': None
    }
}

# 应用配置
APP_CONFIG = {
    'debug': True,
    'host': '0.0.0.0',
    'port': 5000,
    'secret_key': 'your-secret-key',
    'session_timeout': 3600
}

# 日志配置
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'app.log'
} 