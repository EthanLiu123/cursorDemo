from flask import Blueprint, request, jsonify
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.datastream import StreamExecutionEnvironment
import os
from config import FLINK_CLUSTER_CONFIG

flink_bp = Blueprint('flink', __name__)

# Flink环境变量
flink_env = None
table_env = None

@flink_bp.route('/flink-connect', methods=['POST'])
def flink_connect():
    try:
        global flink_env, table_env
        
        # 设置环境变量
        os.environ['JOB_MANAGER_RPC_ADDRESS'] = FLINK_CLUSTER_CONFIG['jobmanager'].split(':')[0]
        os.environ['JOB_MANAGER_RPC_PORT'] = FLINK_CLUSTER_CONFIG['jobmanager'].split(':')[1]
        
        # 创建Flink环境
        settings = EnvironmentSettings.in_streaming_mode()
        flink_env = StreamExecutionEnvironment.get_execution_environment()
        
        # 设置并行度
        flink_env.set_parallelism(FLINK_CLUSTER_CONFIG['parallelism'])
        
        # 设置内存配置
        configuration = flink_env.get_config()
        configuration.set("taskmanager.memory.process.size", FLINK_CLUSTER_CONFIG['memory']['taskmanager'])
        configuration.set("jobmanager.memory.process.size", FLINK_CLUSTER_CONFIG['memory']['jobmanager'])
        
        # 创建Table环境
        table_env = TableEnvironment.create(settings)
        
        # 设置作业名称
        table_env.get_config().get_configuration().set(
            "pipeline.name", 
            "Flink Pipeline"
        )
        
        # 设置REST API地址
        table_env.get_config().get_configuration().set(
            "rest.address",
            FLINK_CLUSTER_CONFIG['rest_api'].split('//')[1].split(':')[0]
        )
        table_env.get_config().get_configuration().set(
            "rest.port",
            FLINK_CLUSTER_CONFIG['rest_api'].split(':')[-1]
        )
        
        # 设置安全配置
        if FLINK_CLUSTER_CONFIG['security']['enabled']:
            if FLINK_CLUSTER_CONFIG['security']['username'] and FLINK_CLUSTER_CONFIG['security']['password']:
                table_env.get_config().get_configuration().set(
                    "security.username",
                    FLINK_CLUSTER_CONFIG['security']['username']
                )
                table_env.get_config().get_configuration().set(
                    "security.password",
                    FLINK_CLUSTER_CONFIG['security']['password']
                )
            
            if FLINK_CLUSTER_CONFIG['security']['kerberos']['enabled']:
                table_env.get_config().get_configuration().set(
                    "security.kerberos.login.keytab",
                    FLINK_CLUSTER_CONFIG['security']['kerberos']['keytab']
                )
                table_env.get_config().get_configuration().set(
                    "security.kerberos.login.principal",
                    FLINK_CLUSTER_CONFIG['security']['kerberos']['principal']
                )
        
        # 设置高可用配置
        if FLINK_CLUSTER_CONFIG['high_availability']['enabled']:
            table_env.get_config().get_configuration().set(
                "high-availability",
                "zookeeper"
            )
            table_env.get_config().get_configuration().set(
                "high-availability.zookeeper.quorum",
                FLINK_CLUSTER_CONFIG['high_availability']['zookeeper_quorum']
            )
            table_env.get_config().get_configuration().set(
                "high-availability.zookeeper.path.root",
                FLINK_CLUSTER_CONFIG['high_availability']['zookeeper_root']
            )
        
        return jsonify({
            "success": True, 
            "message": "连接成功",
            "config": {
                "jobmanager": FLINK_CLUSTER_CONFIG['jobmanager'],
                "parallelism": FLINK_CLUSTER_CONFIG['parallelism'],
                "memory": FLINK_CLUSTER_CONFIG['memory'],
                "security": {
                    "enabled": FLINK_CLUSTER_CONFIG['security']['enabled'],
                    "kerberos_enabled": FLINK_CLUSTER_CONFIG['security']['kerberos']['enabled']
                },
                "high_availability": {
                    "enabled": FLINK_CLUSTER_CONFIG['high_availability']['enabled']
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
        return jsonify({
            "success": True,
            "config": FLINK_CLUSTER_CONFIG
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}) 