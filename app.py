from flask import Flask
import os
from blueprints.main import main_bp
from blueprints.flink import flink_bp
from blueprints.generator import generator_bp

app = Flask(__name__)

# 注册蓝图
app.register_blueprint(main_bp)
app.register_blueprint(flink_bp)
app.register_blueprint(generator_bp)

# 打印当前工作目录和模板目录
print(f"Current working directory: {os.getcwd()}")
print(f"Template directory: {os.path.join(os.getcwd(), 'templates')}")
print(f"Static directory: {os.path.join(os.getcwd(), 'static')}")

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 