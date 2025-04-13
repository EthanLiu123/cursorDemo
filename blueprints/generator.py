from flask import Blueprint, render_template, request
from utils import generate_flinksql, distribute_code
import traceback

generator_bp = Blueprint('generator', __name__)

@generator_bp.route('/code-generator', methods=['GET', 'POST'])
def code_generator():
    try:
        print("Accessing code-generator route")
        output = ""
        if request.method == 'POST':
            input_text = request.form.get('input_text', '')
            output = distribute_code(input_text)
        return render_template('code_generator.html', output=output)
    except Exception as e:
        print(f"Error in code_generator route: {str(e)}")
        print(traceback.format_exc())
        return "页面加载出错，请检查服务器日志", 500

@generator_bp.route('/flinksql-generator', methods=['GET', 'POST'])
def flinksql_generator():
    try:
        print("Accessing flinksql-generator route")
        output = ""
        if request.method == 'POST':
            env = request.form.get('env')
            mode = request.form.get('mode')
            source_type = request.form.get('source_type')
            sink_type = request.form.get('sink_type')
            source_ddl = request.form.get('source_ddl')
            
            # 生成FlinkSQL
            output = generate_flinksql(env, mode, source_type, sink_type, source_ddl)
        
        return render_template('flinksql_generator.html', output=output)
    except Exception as e:
        print(f"Error in flinksql_generator route: {str(e)}")
        print(traceback.format_exc())
        return "页面加载出错，请检查服务器日志", 500 