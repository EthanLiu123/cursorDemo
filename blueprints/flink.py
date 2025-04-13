from flask import Blueprint, request, jsonify
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.datastream import StreamExecutionEnvironment
import os
from config import FLINK_CLUSTER_CONFIGS

flink_bp = Blueprint('flink', __name__)

# Flink环境变量
flink_env = None
table_env = None

@flink_bp.route('/flink-connect', methods=['POST'])
def flink_connect():
    try:
        data = request.get_json()
        env = data.get('env', 'test')
        
        global flink_env, table_env
        
        # 获取集群配置
        config = FLINK_CLUSTER_CONFIGS.get(env)
        if not config:
            return jsonify({"success": False, "message": f"未找到环境 {env} 的配置"})
        
        # 设置环境变量
        os.environ['JOB_MANAGER_RPC_ADDRESS'] = config['jobmanager'].split(':')[0]
        os.environ['JOB_MANAGER_RPC_PORT'] = config['jobmanager'].split(':')[1]
        
        # 创建Flink环境
        settings = EnvironmentSettings.in_streaming_mode()
        flink_env = StreamExecutionEnvironment.get_execution_environment()
        
        # 设置并行度
        flink_env.set_parallelism(config['parallelism'])
        
        # 设置内存配置
        flink_env.get_config().set_taskmanager_memory(config['memory']['taskmanager'])
        flink_env.get_config().set_jobmanager_memory(config['memory']['jobmanager'])
        
        # 创建Table环境
        table_env = TableEnvironment.create(settings)
        
        # 设置作业名称
        table_env.get_config().get_configuration().set_string(
            "pipeline.name", 
            f"{env.capitalize()} Pipeline"
        )
        
        # 设置REST API地址
        table_env.get_config().get_configuration().set_string(
            "rest.address",
            config['rest_api'].split('//')[1].split(':')[0]
        )
        table_env.get_config().get_configuration().set_string(
            "rest.port",
            config['rest_api'].split(':')[-1]
        )
        
        # 设置安全配置
        if config['security']['enabled']:
            if config['security']['username'] and config['security']['password']:
                table_env.get_config().get_configuration().set_string(
                    "security.username",
                    config['security']['username']
                )
                table_env.get_config().get_configuration().set_string(
                    "security.password",
                    config['security']['password']
                )
            
            if config['security']['kerberos']['enabled']:
                table_env.get_config().get_configuration().set_string(
                    "security.kerberos.login.keytab",
                    config['security']['kerberos']['keytab']
                )
                table_env.get_config().get_configuration().set_string(
                    "security.kerberos.login.principal",
                    config['security']['kerberos']['principal']
                )
        
        # 设置高可用配置
        if config['high_availability']['enabled']:
            table_env.get_config().get_configuration().set_string(
                "high-availability",
                "zookeeper"
            )
            table_env.get_config().get_configuration().set_string(
                "high-availability.zookeeper.quorum",
                config['high_availability']['zookeeper_quorum']
            )
            table_env.get_config().get_configuration().set_string(
                "high-availability.zookeeper.path.root",
                config['high_availability']['zookeeper_root']
            )
        
        return jsonify({
            "success": True, 
            "message": "连接成功",
            "config": {
                "jobmanager": config['jobmanager'],
                "parallelism": config['parallelism'],
                "memory": config['memory'],
                "security": {
                    "enabled": config['security']['enabled'],
                    "kerberos_enabled": config['security']['kerberos']['enabled']
                },
                "high_availability": {
                    "enabled": config['high_availability']['enabled']
                }
            }
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@flink_bp.route('/flink-disconnect', methods=['POST'])
def flink_disconnect():
    try:
        global flink_env, table_env
        flink_env = None
        table_env = None
        
        # 清除环境变量
        if 'JOB_MANAGER_RPC_ADDRESS' in os.environ:
            del os.environ['JOB_MANAGER_RPC_ADDRESS']
        if 'JOB_MANAGER_RPC_PORT' in os.environ:
            del os.environ['JOB_MANAGER_RPC_PORT']
            
        return jsonify({"success": True, "message": "断开连接成功"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@flink_bp.route('/flink-execute', methods=['POST'])
def flink_execute():
    try:
        if not table_env:
            return jsonify({"success": False, "message": "未连接到Flink环境"})
        
        data = request.get_json()
        sql = data.get('sql', '')
        
        if not sql:
            return jsonify({"success": False, "message": "SQL语句不能为空"})
        
        # 执行SQL
        result = table_env.execute_sql(sql)
        
        # 获取结果
        if result.get_job_client().get_job_status().name() == "FINISHED":
            # 如果是查询语句，获取结果
            if sql.strip().upper().startswith("SELECT"):
                result_set = result.collect()
                return jsonify({
                    "success": True,
                    "result": [dict(row) for row in result_set]
                })
            else:
                return jsonify({
                    "success": True,
                    "result": [],
                    "message": "SQL执行成功"
                })
        else:
            return jsonify({
                "success": False,
                "message": "SQL执行失败"
            })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@flink_bp.route('/flink-config', methods=['GET'])
def get_flink_config():
    """获取Flink集群配置信息"""
    try:
        env = request.args.get('env', 'test')
        config = FLINK_CLUSTER_CONFIGS.get(env)
        if not config:
            return jsonify({"success": False, "message": f"未找到环境 {env} 的配置"})
        
        return jsonify({
            "success": True,
            "config": config
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}) 