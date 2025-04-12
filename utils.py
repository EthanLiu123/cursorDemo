import re

def distribute_code(code:str)->str:
    return f"这是我的代码解析模块:{code}"

def generate_flinksql(env, mode, source_type, sink_type, source_ddl):
    """生成FlinkSQL
    
    Args:
        env (str): 环境类型，test或prod
        mode (str): 模式类型，batch或stream
        source_type (str): 源类型
        sink_type (str): 目标类型
        source_ddl (str): 源表DDL
        
    Returns:
        str: 生成的FlinkSQL
    """
    # 解析源表DDL
    table_name = extract_table_name(source_ddl)
    columns = extract_columns(source_ddl)
    
    # 根据环境选择不同的配置
    if env == 'test':
        source_config = get_test_config(source_type)
        sink_config = get_test_config(sink_type)
    else:
        source_config = get_prod_config(source_type)
        sink_config = get_prod_config(sink_type)
    
    # 生成FlinkSQL
    sql = f"""-- 源表定义
CREATE TABLE source_table (
    {',\n    '.join(columns)}
) WITH (
    {format_connector_config(source_type, source_config)}
);

-- 目标表定义
CREATE TABLE sink_table (
    {',\n    '.join(columns)}
) WITH (
    {format_connector_config(sink_type, sink_config)}
);

-- 数据同步SQL
{'INSERT INTO sink_table SELECT * FROM source_table;' if mode == 'batch' else 'INSERT INTO sink_table SELECT * FROM source_table;'}
"""
    return sql

def extract_table_name(ddl):
    """从DDL中提取表名
    
    Args:
        ddl (str): DDL语句
        
    Returns:
        str: 表名
    """
    match = re.search(r'CREATE\s+TABLE\s+(\w+)', ddl, re.IGNORECASE)
    return match.group(1) if match else 'unknown_table'

def extract_columns(ddl):
    """从DDL中提取列定义
    
    Args:
        ddl (str): DDL语句
        
    Returns:
        list: 列定义列表
    """
    columns = []
    for line in ddl.split('\n'):
        line = line.strip()
        if line and not line.startswith('CREATE') and not line.startswith(')') and not line.startswith('('):
            columns.append(line.rstrip(','))
    return columns

def get_test_config(connector_type):
    """获取测试环境配置
    
    Args:
        connector_type (str): 连接器类型
        
    Returns:
        dict: 配置字典
    """
    configs = {
        'mysql': {
            'url': 'jdbc:mysql://test-mysql:3306/test',
            'username': 'test',
            'password': 'test123'
        },
        'oracle': {
            'url': 'jdbc:oracle:thin:@test-oracle:1521:orcl',
            'username': 'test',
            'password': 'test123'
        },
        'paimon': {
            'warehouse': 'hdfs://test-namenode:8020/paimon',
            'database': 'test',
            'table': 'test_table'
        },
        'pgsql': {
            'url': 'jdbc:postgresql://test-pgsql:5432/test',
            'username': 'test',
            'password': 'test123'
        },
        'doris': {
            'url': 'jdbc:mysql://test-doris:9030/test',
            'username': 'test',
            'password': 'test123'
        },
        'kafka': {
            'bootstrap.servers': 'test-kafka:9092',
            'topic': 'test_topic',
            'format': 'json'
        },
        'tidb': {
            'url': 'jdbc:mysql://test-tidb:4000/test',
            'username': 'test',
            'password': 'test123'
        }
    }
    return configs.get(connector_type, {})

def get_prod_config(connector_type):
    """获取生产环境配置
    
    Args:
        connector_type (str): 连接器类型
        
    Returns:
        dict: 配置字典
    """
    configs = {
        'mysql': {
            'url': 'jdbc:mysql://prod-mysql:3306/prod',
            'username': 'prod',
            'password': 'prod123'
        },
        'oracle': {
            'url': 'jdbc:oracle:thin:@prod-oracle:1521:orcl',
            'username': 'prod',
            'password': 'prod123'
        },
        'paimon': {
            'warehouse': 'hdfs://prod-namenode:8020/paimon',
            'database': 'prod',
            'table': 'prod_table'
        },
        'pgsql': {
            'url': 'jdbc:postgresql://prod-pgsql:5432/prod',
            'username': 'prod',
            'password': 'prod123'
        },
        'doris': {
            'url': 'jdbc:mysql://prod-doris:9030/prod',
            'username': 'prod',
            'password': 'prod123'
        },
        'kafka': {
            'bootstrap.servers': 'prod-kafka:9092',
            'topic': 'prod_topic',
            'format': 'json'
        },
        'tidb': {
            'url': 'jdbc:mysql://prod-tidb:4000/prod',
            'username': 'prod',
            'password': 'prod123'
        }
    }
    return configs.get(connector_type, {})

def format_connector_config(connector_type, config):
    """格式化连接器配置
    
    Args:
        connector_type (str): 连接器类型
        config (dict): 配置字典
        
    Returns:
        str: 格式化后的配置字符串
    """
    if connector_type == 'kafka':
        return f"""'connector' = 'kafka',
    'topic' = '{config['topic']}',
    'properties.bootstrap.servers' = '{config['bootstrap.servers']}',
    'format' = '{config['format']}'"""
    elif connector_type == 'paimon':
        return f"""'connector' = 'paimon',
    'warehouse' = '{config['warehouse']}',
    'database' = '{config['database']}',
    'table' = '{config['table']}'"""
    else:
        return f"""'connector' = 'jdbc',
    'url' = '{config['url']}',
    'username' = '{config['username']}',
    'password' = '{config['password']}'"""
