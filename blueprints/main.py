from flask import Blueprint, render_template

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    try:
        print("Accessing index route")
        return render_template('flink_shell.html')
    except Exception as e:
        print(f"Error in index route: {str(e)}")
        return "页面加载出错，请检查服务器日志", 500 