
from flask import Flask, render_template, request, jsonify,session
import requests
import json
from cassandra.cluster import Cluster
import csv

deck_url_template = 'http://deckofcardsapi.com/api/deck/new/shuffle/?deck_count=1'

cluster = Cluster(contact_points=['172.17.0.2'], port=9042)
session = cluster.connect()
app = Flask(__name__)


@app.route('/deck', methods=['POST','GET'])
def getdeck():
    response = requests.get(deck_url_template)
    if response.ok:
        parsed = response.json()
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>",parsed)

        insert = session.execute("""INSERT INTO deck.data (success, id, shuffled, remainings) VALUES ({}, '{}', {}, {})""".format(parsed['success'], parsed['deck_id'], parsed['shuffled'], parsed['remaining']))
        return jsonify(parsed)
    else:
        return jsonify(response.reason)

@app.route('/getdeck', methods=['GET'])
def deck():
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    rows = session.execute("""SELECT * from deck.data""")
    print(rows._current_rows)
    k=rows._current_rows
    if rows:
        return jsonify({'data':k}), 200
    return jsonify({'error':'No data present'}), 404

@app.route('/getdeck/<id>', methods=['GET'])
def one_deck(id):
    rows = session.execute("""SELECT * FROM deck.data where id = '{}' ALLOW FILTERING""".format(id))
    print('>>>>>>>>>>>>>>>>>>>>>>>',rows._current_rows)
    if rows:
        return jsonify({'data':rows[0]}), 200
    return jsonify({'message':'Card for this ID does not exist'}), 404

@app.route('/getdeck/update/<success>/<id>/<shuffled>/<remaining>/', methods=['PUT','GET'])
def update_characteristics(success, id, shuffled, remaining):

    rows = session.execute("""SELECT * FROM deck.data WHERE id='{}' ALLOW FILTERING""".format(id))
    if not rows:
        return jsonify({'error':'No such ID exists'})
    rows_update = session.execute("""UPDATE deck.data set success={}, shuffled={}, remainings={} WHERE id='{}'""".format(success, shuffled, remaining, id))
    return jsonify({'ID':id, 'success':success, 'shuffle':shuffled, 'Remaining':remaining}), 200

@app.route('/getdata/delete/<id>', methods=['DELETE','GET'])
def delete_deck(id):
    row=session.execute("""SELECT * FROM deck.data WHERE id='{}'""".format(id))
    if row:
        deletedRows=session.execute("""DELETE from deck.data WHERE id='{}'""".format(id))
        print('>>>>>>>>>>>>>>>>>>>>>>>>',deletedRows._current_rows)
        return jsonify({'ID':id, 'Message':'Deleted'}), 200
    return jsonify({"message":"The id doesn't exist"}), 404


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
