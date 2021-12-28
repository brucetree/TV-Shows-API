import sqlite3
import json
from flask import *
from flask_restx import *
import urllib.request as req
import time
from datetime import datetime
import matplotlib.pyplot as plt


def execute_sql(command):
    con = sqlite3.connect('z5253945.db', check_same_thread=False)
    cursor = con.cursor()
    if len(command) > 1:
        cursor.executemany(command[0], command[1])
    else:
        cursor.execute(command[0])
    result = cursor.fetchall()
    con.commit()
    con.close()
    return result

global id_list
id_list = []

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
api = Api(app,default="TV Shows",
          title="TV Dataset",
          description="This is a database of tv-shows.")

# tv_model='/tv-shows/import?name='
parser = api.parser()
parser.add_argument('name', type=str, help='only for post', location='args')

patch_model = api.model('Payload', {
    'name': fields.String
})

@api.route('/tv-shows/import')
@api.response(201, 'Created')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
class TV_shows(Resource):
    @api.doc(params={'name': 'TV-show name'},description="Insert a tv show by its name")
    def post(self):
        original_name = parser.parse_args()['name']
        # check name exist in api
        if not original_name:
            return make_response(jsonify({"message": "no name entered"}),404)
        name = original_name.replace(' ', '%20')
        name = name.lower()
        resource = req.Request('http://api.tvmaze.com/search/shows?q={}'.format(name))
        data = json.loads(req.urlopen(resource).read())
        # exact search
        if not data or not data[0] or not data[0]['show'] or not data[0]['show']['name']:
            return make_response(jsonify({"message": "{} does not exist".format(original_name)}), 404)
        if data[0]['show']['name'].lower() != original_name.lower():
            return make_response(jsonify({"message": "{} does not exist".format(original_name)}), 404)
        # check id exist in database
        tvmaze_id = data[0]['show']['id']
        true_name = data[0]['show']['name']
        local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        type = data[0]['show']['type']
        language = data[0]['show']['language']
        genres = ".".join(data[0]['show']['genres'])
        status = data[0]['show']['status']
        runtime = data[0]['show']['runtime']
        premiered = data[0]['show']['premiered']
        officialSite = data[0]['show']['officialSite']
        schedule = str(data[0]['show']['schedule'])
        # change rating none value
        rating = data[0]['show']['rating']
        if rating['average'] is None:
            rating = {'average': 0}
        if runtime is None:
            runtime=0
        rating = str(rating)
        weight = data[0]['show']['weight']
        if weight is None:
            weight=0
        network = str(data[0]['show']['network'])
        summary = data[0]['show']['summary']
        _links = 'this is a link'
        insert_list1 = [(local_time, tvmaze_id, true_name, type, language, genres, status, runtime, premiered,
                         officialSite, schedule, rating, weight, network, summary, _links), ]

        execute_sql([
            "INSERT OR IGNORE INTO TV_show('last_update','tvmaze_id','name','type','language','genres','status','runtime','premiered','officialSite','schedule','rating','weight','network','summary','_links') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            insert_list1])

        result = execute_sql([
            "SELECT TV_show.id,TV_show.last_update,TV_show.tvmaze_id FROM TV_show WHERE tvmaze_id={}".format(
                tvmaze_id)])

        if result[0][0] in id_list:
            id_value = result[0][0]
            id_index = id_list.index(id_value)
            self_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id_value)
            if id_index == 0:
                if len(id_list) > 1:
                    next_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id_value + 1)
                    link = {
                        "self": {
                            "href": "{}".format(self_link)
                        },
                        "next": {
                            "href": "{}".format(next_link)
                        }
                    }

                    return make_response(jsonify({
                        "id": "{}".format(result[0][0]),
                        "last-update": "{}".format(result[0][1]),
                        "tvmaze-id": "{}".format(result[0][2]),
                        "_links": link
                    }), 200)
                else:
                    link = {
                        "self": {
                            "href": "{}".format(self_link)
                        }
                    }

                    return make_response(jsonify({
                        "id": "{}".format(result[0][0]),
                        "last-update": "{}".format(result[0][1]),
                        "tvmaze-id": "{}".format(result[0][2]),
                        "_links": link
                    }), 200)
            elif id_index > 0 and id_index < len(id_list) - 1:
                pre_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id_value - 1)
                next_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id_value + 1)
                link = {
                    "self": {
                        "href": "{}".format(self_link)
                    },
                    "previous": {
                        "href": "{}".format(pre_link)
                    },
                    "next": {
                        "href": "{}".format(next_link)
                    }
                }

                return make_response(jsonify({
                    "id": "{}".format(result[0][0]),
                    "last-update": "{}".format(result[0][1]),
                    "tvmaze-id": "{}".format(result[0][2]),
                    "_links": link
                }), 200)
            elif id_index == len(id_list) - 1:
                pre_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id_value - 1)
                link = {
                    "self": {
                        "href": "{}".format(self_link)
                    },
                    "previous": {
                        "href": "{}".format(pre_link)
                    },
                }

                return make_response(jsonify({
                    "id": "{}".format(result[0][0]),
                    "last-update": "{}".format(result[0][1]),
                    "tvmaze-id": "{}".format(result[0][2]),
                    "_links": link
                }), 200)

        else:
            if len(id_list) == 0:
                # append id
                id_list.append(result[0][0])
                self_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(result[0][0])
                link = {
                    "self": {
                        "href": "{}".format(self_link)
                    }
                }

                return make_response(jsonify({
                    "id": "{}".format(result[0][0]),
                    "last-update": "{}".format(result[0][1]),
                    "tvmaze-id": "{}".format(result[0][2]),
                    "_links": link
                }), 201)
            if len(id_list) != 0:
                pre_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(result[0][0] - 1)
                self_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(result[0][0])
                # append id
                id_list.append(result[0][0])
                link = {
                    "self": {
                        "href": "{}".format(self_link)
                    },
                    "previous": {
                        "href": "{}".format(pre_link)
                    },
                }

                return make_response(jsonify({
                    "id": "{}".format(result[0][0]),
                    "last-update": "{}".format(result[0][1]),
                    "tvmaze-id": "{}".format(result[0][2]),
                    "_links": link
                }), 201)


@api.route("/tv-shows/<int:id>")
class Q2(Resource):
    @api.doc(description="Search a tv show by its ID")
    def get(self, id):
        get_id = execute_sql(["SELECT id FROM TV_show WHERE id={}".format(id)])
        id_select_list = execute_sql(["SELECT id FROM TV_show"])
        id_temp = []
        for index in id_select_list:
            id_temp.append(index[0])
        id_temp_list = sorted(id_temp)
        if not get_id:
            return make_response(jsonify({"message": "ID {} doesn't exist".format(id)}), 404)
        else:
            data = execute_sql([
                "SELECT tvmaze_id,id,last_update,name,type,language,genres,status,runtime,premiered,officialSite,schedule,rating,weight,network,summary FROM TV_show WHERE id={}".format(
                    id)])
            self_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(data[0][1])
            genres = data[0][6].split(".")
            runtime = int(data[0][8])
            schedule = eval(data[0][11])
            weight = int(data[0][13])
            rating = eval(data[0][12])
            network = eval(data[0][14])
            link = {
                "self": {
                    "href": "{}".format(self_link)
                }
            }
            if id in id_temp_list:
                id_index = id_temp_list.index(id)
                self_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id)
                if id_index == 0:
                    if len(id_temp_list) > 1:
                        next_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id + 1)
                        link = {
                            "self": {
                                "href": "{}".format(self_link)
                            },
                            "next": {
                                "href": "{}".format(next_link)
                            }
                        }

                    else:
                        link = {
                            "self": {
                                "href": "{}".format(self_link)
                            }
                        }


                elif id_index > 0 and id_index < len(id_temp_list) - 1:
                    pre_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id - 1)
                    next_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id + 1)
                    link = {
                        "self": {
                            "href": "{}".format(self_link)
                        },
                        "previous": {
                            "href": "{}".format(pre_link)
                        },
                        "next": {
                            "href": "{}".format(next_link)
                        }
                    }

                elif id_index == len(id_temp_list) - 1:
                    pre_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id - 1)
                    link = {
                        "self": {
                            "href": "{}".format(self_link)
                        },
                        "previous": {
                            "href": "{}".format(pre_link)
                        },
                    }

            result = jsonify({
                "tvmaze-id": data[0][0],
                "id": int(data[0][1]),
                "last-update": "{}".format(data[0][2]),
                "name": "{}".format(data[0][3]),
                "type": "{}".format(data[0][4]),
                "language": "{}".format(data[0][5]),
                "genres": genres,
                "status": "{}".format(data[0][7]),
                "runtime": runtime,
                "premiered": "{}".format(data[0][9]),
                "officialSite": "{}".format(data[0][10]),
                "schedule": schedule,
                "rating": rating,
                "weight": weight,
                "network": network,
                "summary": "{}".format(data[0][15]),
                "_links": link
            })
            return make_response(result, 200)

    @api.doc(description="Delete a tv show by its ID")
    def delete(self, id):
        get_id = execute_sql(["SELECT id FROM TV_show WHERE id={}".format(id)])
        if not get_id:
            return make_response(jsonify({"message": "ID {} doesn't exist".format(id)}), 404)

        execute_sql(["DELETE FROM TV_show WHERE id={}".format(id)])
        result = {
            "message": "The tv show with id {} was removed from the database!".format(id),
            "id": id
        }
        return make_response(jsonify(result), 200)

    @api.doc(description="Update a tv show by its ID")
    @api.expect(patch_model)
    def patch(self, id):
        get_id = execute_sql(["SELECT id FROM TV_show WHERE id={}".format(id)])
        id_select_list = execute_sql(["SELECT id FROM TV_show"])
        id_temp = []
        for index in id_select_list:
            id_temp.append(index[0])
        id_temp_list = sorted(id_temp)
        if not get_id:
            return make_response(jsonify({"message": "ID {} doesn't exist".format(id)}), 404)
        payload = request.get_json()
        # for key in payload.keys():
        #     if key not in patch_model.keys():
        #         return make_response(jsonify({"message": "Property {} is invalid".format(key)}), 400)
        for k, v in payload.items():
            if k == "genres":
                v = ".".join(v)
            if k=="network":
                v=str(v)
                v=v.replace('\'','\'\'')
            if k=="rating":
                v = str(v)
                v = v.replace('\'', '\'\'')
            execute_sql(["UPDATE TV_show SET {} ='{}' WHERE id={}".format(k, v, id)])
        local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        execute_sql(["UPDATE TV_show SET {} ='{}' WHERE id ={}".format('last_update', local_time, id)])
        self_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id)
        link = {
            "self": {
                "href": "{}".format(self_link)
            }
        }
        if id in id_temp_list:
            id_index = id_temp_list.index(id)
            self_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id)
            if id_index == 0:
                if len(id_temp_list) > 1:
                    next_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id + 1)
                    link = {
                        "self": {
                            "href": "{}".format(self_link)
                        },
                        "next": {
                            "href": "{}".format(next_link)
                        }
                    }

                else:
                    link = {
                        "self": {
                            "href": "{}".format(self_link)
                        }
                    }


            elif id_index > 0 and id_index < len(id_temp_list) - 1:
                pre_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id - 1)
                next_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id + 1)
                link = {
                    "self": {
                        "href": "{}".format(self_link)
                    },
                    "previous": {
                        "href": "{}".format(pre_link)
                    },
                    "next": {
                        "href": "{}".format(next_link)
                    }
                }

            elif id_index == len(id_temp_list) - 1:
                pre_link = 'http://127.0.0.1:5000/tv-shows/{}'.format(id - 1)
                link = {
                    "self": {
                        "href": "{}".format(self_link)
                    },
                    "previous": {
                        "href": "{}".format(pre_link)
                    },
                }
        result = {
            "id": id,
            "last-update": "{}".format(local_time),
            "_links": link
        }
        return make_response(jsonify(result), 200)


parser_q5 = api.parser()
parser_q5.add_argument('filter', type=str, help='only for get', location='args')
parser_q5.add_argument('page_size', type=int, help='only for get', location='args')
parser_q5.add_argument('page', type=int, help='only for get', location='args')
parser_q5.add_argument('order_by', type=str, help='only for get', location='args')


@api.route('/tv-shows/')
@api.response(201, 'Created')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
class Q5(Resource):
    @api.doc(description="show tv shows by its attribute")
    @api.doc(params={'order_by': 'default : +id'})
    @api.doc(params={'page': 'default : 1'})
    @api.doc(params={'page_size': 'default : 100'})
    @api.doc(params={'filter': 'default : id,name'})
    def get(self):
        order_by = parser_q5.parse_args()['order_by']
        page = parser_q5.parse_args()['page']
        page_size = parser_q5.parse_args()['page_size']
        filter = parser_q5.parse_args()['filter']
        if page is None:
            page = 1
        if order_by is None:
            order_by="+id"
        if page_size is None:
            page_size=100
        if filter is None:
            filter="id,name"
        # order by
        supported_attribute = ['id', 'name', 'runtime', 'premiered', 'rating-average']
        # get all potential data
        data = execute_sql(["SELECT * FROM TV_show"])
        if not data:
            return make_response(jsonify({"message": "database not found"}), 404)
        data_list = []
        for i in range(len(data)):
            genres = data[i][6].split(".")
            runtime = int(data[i][8])
            schedule = eval(data[i][11])
            weight = int(data[i][13])
            rating = eval(data[i][12])
            network = eval(data[i][14])
            temp_dic = {
                "tvmaze-id": data[i][0],
                "id": int(data[i][1]),
                "last-update": "{}".format(data[i][2]),
                "name": "{}".format(data[i][3]),
                "type": "{}".format(data[i][4]),
                "language": "{}".format(data[i][5]),
                "genres": genres,
                "status": "{}".format(data[i][7]),
                "runtime": runtime,
                "premiered": "{}".format(data[i][9]),
                "officialSite": "{}".format(data[i][10]),
                "schedule": schedule,
                "rating": rating,
                "weight": weight,
                "network": network,
                "summary": "{}".format(data[i][15])
            }
            data_list.append(temp_dic)

        if order_by:
            # get command from order_by list
            command_order = order_by.split(',')
            # order not followed by rules return 404
            for index in command_order:
                index1 = index[1:]
                if index1 not in supported_attribute:
                    return make_response(jsonify({"message": "Property {} is invalid in order_by".format(index)}), 400)

            for index in command_order:
                strip_index = index.strip()
                if strip_index[1:] == 'rating-average':
                    if strip_index[0] == '+':
                        # sort date by ascending
                        data_list = sorted(data_list, key=lambda x: x['rating']['average'], reverse=False)
                    if strip_index[0] == '-':
                        # sort date by descending
                        data_list = sorted(data_list, key=lambda x: x['rating']['average'], reverse=True)
                else:
                    if strip_index[0] == '+':
                        # sort date by ascending
                        data_list = sorted(data_list, key=lambda x: x[strip_index[1:]], reverse=False)
                    if strip_index[0] == '-':
                        # sort date by descending
                        data_list = sorted(data_list, key=lambda x: x[strip_index[1:]], reverse=True)
        # filter
        if filter:
            # get command from filter list
            command_filter = filter.split(',')
            # get data by filter
            new_data_by_filter = []
            for index in data_list:
                single_data_dic = {}
                for item in command_filter:
                    for k, v in index.items():
                        if k == item:
                            single_data_dic[item] = v
                new_data_by_filter.append(single_data_dic)
            data_list = new_data_by_filter

        tv_show_result=data_list
        # page and page_size
        num_of_data = len(data_list)
        link = {
            "self": {
                "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(
                    order_by, page, page_size, filter)
            }
        }
        if num_of_data < page_size*page and page > 1:
            return make_response(jsonify({"message": "page number is wrong"}), 404)
        if num_of_data>page_size:
            # data > size
            tv_show_result=data_list[(page-1)*page_size:page*page_size]
            if page ==1:
                # first page
                link = {
                    "self": {
                        "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(order_by,page,page_size,filter)
                    },
                    "next": {
                        "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(order_by,page+1,page_size,filter)
                    }
                }
            if page>1 and page<=int(num_of_data/page_size):
                # middle page
                link = {
                    "self": {
                        "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(order_by,page,page_size,filter)
                    },
                    "previous": {
                        "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(order_by,page-1,page_size,filter)
                    },
                    "next": {
                        "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(order_by,page+1,page_size,filter)
                    }
                }
            if page > int(num_of_data/page_size):
                # latest page
                link = {
                    "self": {
                        "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(
                            order_by, page, page_size, filter)
                    },
                    "previous": {
                        "href": "http://127.0.0.1:5000/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(
                            order_by, page - 1, page_size, filter)
                    }
                }

        result = {
            "page": page,
            "page-size": page_size,
            "tv-shows": tv_show_result,
            "_links": link
        }
        return make_response(jsonify(result), 200)


parser_q6=api.parser()
parser_q6.add_argument('format', type=str, help='only for Q6', location='args')
parser_q6.add_argument('by', type=str, help='only for Q6', location='args')


@api.route('/tv-shows/statistics')
@api.response(201, 'Created')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
class Q6(Resource):
    @api.doc(description="statistic of tv shows")
    @api.doc(params={'format': 'json or image'})
    @api.doc(params={'by': 'attribute'})
    def get(self):
        format = parser_q6.parse_args()['format']
        by = parser_q6.parse_args()['by']
        data_by=execute_sql(["SELECT {},last_update FROM TV_show".format(by)])
        # statistic
        num_data=len(data_by)
        # caculate time
        local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        time_list=[]
        for index_time in data_by:
            time_list.append(index_time[1])
        new_time_list=[]
        for index1 in time_list:
            temp_time=datetime.strptime(index1,'%Y-%m-%d %H:%M:%S')
            temp_local_time=datetime.strptime(local_time,'%Y-%m-%d %H:%M:%S')
            time_diff=(temp_local_time-temp_time).days
            if time_diff<1:
                new_time_list.append(index1)
        time_less_24=len(new_time_list)

        # get genres in every tv
        genres_list=[]
        if by=='genres':
            for index in data_by:
                if index[0]=="":
                    continue
                else:
                    temp_genres_list=index[0].split('.')
                    for i in temp_genres_list:
                        genres_list.append(i)
        else:
            for index in data_by:
                if index[0] is None:
                    continue
                else:
                    genres_list.append(index[0])
        statistic_dict={}
        for key in genres_list:
            statistic_dict[key]=statistic_dict.get(key,0) +1
        # get percentage
        percentage={}
        for k,v in statistic_dict.items():
            percentage[k]=round(v/num_data,1)
        # plot
        if format =="image":
            labels_list=list(percentage.keys())
            values_list=list(percentage.values())

            plt.bar(labels_list,values_list)
            plt.title("total : {} total-updated : {}".format(num_data,time_less_24))
            plt.xlabel(by)
            plt.ylabel("Percentage")
            plt.savefig("z5253945.jpg")
            plt.cla()
            return send_file("z5253945.jpg", mimetype='image/jpg',cache_timeout=0)
        if format=="json":
            result={
                "total": num_data,
                "total-updated": time_less_24,
                "values": percentage
            }
            return make_response(jsonify(result),200)

if __name__ == '__main__':
    con = sqlite3.connect('z5253945.db', check_same_thread=False)
    cursor = con.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS TV_show(
            tvmaze_id INTEGER UNIQUE,
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            last_update text,
            name text,
            type text,
            language text,
            genres text,
            status text,
            runtime real,
            premiered text,
            officialSite text,
            schedule text,
            rating text,
            weight real,
            network text,
            summary text,
            _links text)''')

    app.run(host='127.0.0.1', port=5000, debug=True)
    con.commit()
    con.close()
