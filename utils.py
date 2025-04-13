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
    try:
        # 解析源表DDL
        table_info = parse_ddl(source_ddl)
        if not table_info:
            return "错误：无法解析DDL语句"
        
        # 获取配置
        source_config = get_connector_config(env, source_type, table_info['table_name'])
        sink_config = get_connector_config(env, sink_type, f"{table_info['table_name']}_sink")
        
        # 生成FlinkSQL
        sql = generate_sql(mode, source_type, sink_type, table_info, source_config, sink_config)
        
        return sql
    except Exception as e:
        return f"错误：生成FlinkSQL时发生异常 - {str(e)}"

def parse_ddl(ddl):
    """解析DDL语句
    
    Args:
        ddl (str): DDL语句
        
    Returns:
        dict: 包含表名和列定义的字典
    """
    try:
        # 移除注释
        ddl = remove_comments(ddl)
        
        # 提取表名
        table_name = extract_table_name(ddl)
        if not table_name:
            return None
            
        # 提取列定义
        columns = extract_columns(ddl)
        if not columns:
            return None
            
        # 提取主键
        primary_keys = extract_primary_keys(ddl)
        
        return {
            'table_name': table_name,
            'columns': columns,
            'primary_keys': primary_keys
        }
    except Exception:
        return None

def remove_comments(ddl):
    """移除SQL注释
    
    Args:
        ddl (str): 原始DDL语句
        
    Returns:
        str: 移除注释后的DDL语句
    """
    # 移除单行注释
    ddl = re.sub(r'--.*$', '', ddl, flags=re.MULTILINE)
    # 移除多行注释
    ddl = re.sub(r'/\*.*?\*/', '', ddl, flags=re.DOTALL)
    return ddl.strip()

def extract_table_name(ddl):
    """从DDL中提取表名
    
    Args:
        ddl (str): DDL语句
        
    Returns:
        str: 表名
    """
    # 支持带schema的表名
    match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\w.]+)', ddl, re.IGNORECASE)
    if not match:
        return None
    
    table_name = match.group(1)
    # 移除反引号
    table_name = table_name.replace('`', '')
    # 如果有schema，只取表名部分
    if '.' in table_name:
        table_name = table_name.split('.')[-1]
    return table_name

def extract_columns(ddl):
    """从DDL中提取列定义
    
    Args:
        ddl (str): DDL语句
        
    Returns:
        list: 列定义列表
    """
    # 提取括号内的内容
    match = re.search(r'\((.*)\)', ddl, re.DOTALL)
    if not match:
        return None
        
    column_text = match.group(1)
    columns = []
    
    # 分割列定义
    for line in column_text.split(','):
        line = line.strip()
        if not line or line.upper().startswith(('PRIMARY KEY', 'UNIQUE', 'INDEX', 'KEY')):
            continue
            
        # 处理列定义
        col_match = re.match(r'`?(\w+)`?\s+([\w\(\)]+)(?:\s+COMMENT\s+\'([^\']+)\')?', line)
        if col_match:
            col_name = col_match.group(1)
            col_type = col_match.group(2)
            col_comment = col_match.group(3) if col_match.group(3) else ''
            
            # 格式化列定义
            if col_comment:
                columns.append(f"`{col_name}` {col_type} COMMENT '{col_comment}'")
            else:
                columns.append(f"`{col_name}` {col_type}")
    
    return columns

def extract_primary_keys(ddl):
    """从DDL中提取主键定义
    
    Args:
        ddl (str): DDL语句
        
    Returns:
        list: 主键列表
    """
    # 查找PRIMARY KEY定义
    match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', ddl, re.IGNORECASE)
    if not match:
        return []
        
    # 处理主键列
    pk_columns = match.group(1)
    return [col.strip('` ') for col in pk_columns.split(',')]

def get_connector_config(env, connector_type, table_name):
    """获取连接器配置
    
    Args:
        env (str): 环境类型
        connector_type (str): 连接器类型
        table_name (str): 表名
        
    Returns:
        dict: 配置字典
    """
    base_config = get_base_config(env, connector_type)
    
    # 根据连接器类型添加特定配置
    if connector_type == 'kafka':
        base_config.update({
            'topic': f"{env}_{table_name}",
            'format': 'json',
            'scan.startup.mode': 'earliest-offset' if env == 'test' else 'latest-offset'
        })
    elif connector_type == 'paimon':
        base_config.update({
            'table': table_name
        })
    else:
        base_config.update({
            'table-name': table_name
        })
    
    return base_config

def get_base_config(env, connector_type):
    """获取基础配置
    
    Args:
        env (str): 环境类型
        connector_type (str): 连接器类型
        
    Returns:
        dict: 配置字典
    """
    configs = {
        'test': {
            'mysql': {
                'connector': 'jdbc',
                'driver': 'com.mysql.cj.jdbc.Driver',
                'url': 'jdbc:mysql://test-mysql:3306/test',
                'username': 'test',
                'password': 'test123'
            },
            'oracle': {
                'connector': 'jdbc',
                'driver': 'oracle.jdbc.driver.OracleDriver',
                'url': 'jdbc:oracle:thin:@test-oracle:1521:orcl',
                'username': 'test',
                'password': 'test123'
            },
            'paimon': {
                'connector': 'paimon',
                'warehouse': 'hdfs://test-namenode:8020/paimon',
                'database': 'test'
            },
            'pgsql': {
                'connector': 'jdbc',
                'driver': 'org.postgresql.Driver',
                'url': 'jdbc:postgresql://test-pgsql:5432/test',
                'username': 'test',
                'password': 'test123'
            },
            'doris': {
                'connector': 'jdbc',
                'driver': 'com.mysql.cj.jdbc.Driver',
                'url': 'jdbc:mysql://test-doris:9030/test',
                'username': 'test',
                'password': 'test123'
            },
            'kafka': {
                'connector': 'kafka',
                'properties.bootstrap.servers': 'test-kafka:9092',
                'properties.group.id': 'test_group'
            },
            'tidb': {
                'connector': 'jdbc',
                'driver': 'com.mysql.cj.jdbc.Driver',
                'url': 'jdbc:mysql://test-tidb:4000/test',
                'username': 'test',
                'password': 'test123'
            }
        },
        'prod': {
            'mysql': {
                'connector': 'jdbc',
                'driver': 'com.mysql.cj.jdbc.Driver',
                'url': 'jdbc:mysql://prod-mysql:3306/prod',
                'username': 'prod',
                'password': 'prod123'
            },
            'oracle': {
                'connector': 'jdbc',
                'driver': 'oracle.jdbc.driver.OracleDriver',
                'url': 'jdbc:oracle:thin:@prod-oracle:1521:orcl',
                'username': 'prod',
                'password': 'prod123'
            },
            'paimon': {
                'connector': 'paimon',
                'warehouse': 'hdfs://prod-namenode:8020/paimon',
                'database': 'prod'
            },
            'pgsql': {
                'connector': 'jdbc',
                'driver': 'org.postgresql.Driver',
                'url': 'jdbc:postgresql://prod-pgsql:5432/prod',
                'username': 'prod',
                'password': 'prod123'
            },
            'doris': {
                'connector': 'jdbc',
                'driver': 'com.mysql.cj.jdbc.Driver',
                'url': 'jdbc:mysql://prod-doris:9030/prod',
                'username': 'prod',
                'password': 'prod123'
            },
            'kafka': {
                'connector': 'kafka',
                'properties.bootstrap.servers': 'prod-kafka:9092',
                'properties.group.id': 'prod_group'
            },
            'tidb': {
                'connector': 'jdbc',
                'driver': 'com.mysql.cj.jdbc.Driver',
                'url': 'jdbc:mysql://prod-tidb:4000/prod',
                'username': 'prod',
                'password': 'prod123'
            }
        }
    }
    
    return configs[env][connector_type]

def format_connector_config(config):
    """格式化连接器配置
    
    Args:
        config (dict): 配置字典
        
    Returns:
        str: 格式化后的配置字符串
    """
    return ',\n    '.join(f"'{k}' = '{v}'" for k, v in config.items())

def generate_sql(mode, source_type, sink_type, table_info, source_config, sink_config):
    """生成FlinkSQL语句
    
    Args:
        mode (str): 模式类型
        source_type (str): 源类型
        sink_type (str): 目标类型
        table_info (dict): 表信息
        source_config (dict): 源配置
        sink_config (dict): 目标配置
        
    Returns:
        str: FlinkSQL语句
    """
    # 生成WITH子句
    source_with = format_connector_config(source_config)
    sink_with = format_connector_config(sink_config)
    
    # 生成列定义
    columns_def = ',\n    '.join(table_info['columns'])
    
    # 添加主键定义
    if table_info['primary_keys']:
        primary_key_def = f",\n    PRIMARY KEY ({', '.join(table_info['primary_keys'])}) NOT ENFORCED"
    else:
        primary_key_def = ""
    
    # 生成完整SQL
    sql = f"""-- 源表定义
CREATE TABLE source_table (
    {columns_def}{primary_key_def}
) WITH (
    {source_with}
);

-- 目标表定义
CREATE TABLE sink_table (
    {columns_def}{primary_key_def}
) WITH (
    {sink_with}
);

-- 数据同步SQL
INSERT INTO sink_table
SELECT * FROM source_table;"""

    return sql
