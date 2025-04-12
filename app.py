from flask import Flask, render_template, request
from utils import generate_flinksql

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/text-process')
def text_process():
    return render_template('text_process.html')

@app.route('/code-generator')
def code_generator():
    return render_template('code_generator.html')

@app.route('/flinksql-generator', methods=['GET', 'POST'])
def flinksql_generator():
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

if __name__ == '__main__':
    app.run(debug=True) 