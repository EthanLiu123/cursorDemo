from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    output = ""
    if request.method == 'POST':
        input_text = request.form.get('input_text', '')
        output = f"您输入的内容是: {input_text}"
    return render_template('index.html', output=output)

if __name__ == '__main__':
    app.run(debug=True) 