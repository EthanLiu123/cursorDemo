from flask import Flask, render_template, request
from utils import distribute_code
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    output = ""
    if request.method == 'POST':
        input_text = request.form.get('input_text', '')
        output =distribute_code(input_text)
    return render_template('index.html', output=output)

if __name__ == '__main__':
    print("tets")
    app.run(debug=True) 