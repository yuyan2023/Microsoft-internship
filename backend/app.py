from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import pymysql

# 创建 Flask 应用
app = Flask(__name__)
CORS(app)

# MySQL 连接配置（请替换为你的数据库信息）
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:321227@localhost:3306/spider_data?charset=utf8mb4'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# 初始化数据库
db = SQLAlchemy(app)


# 电影数据模型
class Movie(db.Model):
    __tablename__ = 'movies'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    title = db.Column(db.String(255), nullable=False)
    rating = db.Column(db.Numeric(3, 1))
    num_raters = db.Column(db.Integer)
    quote = db.Column(db.Text)
    director = db.Column(db.String(255))
    actors = db.Column(db.Text)  # 可能有多个演员，用逗号分隔
    release_date = db.Column(db.String(50))  # 修改为字符串类型，因为数据中可能有非数字年份
    genres = db.Column(db.Text)  # 可能有多个类型
    link = db.Column(db.String(255))


# API: 首页测试
@app.route('/')
def home():
    return jsonify({"message": "Flask API is running!"})


# API: 获取所有电影（支持分页）
@app.route('/movies', methods=['GET'])
def get_movies():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)

    movies = Movie.query.paginate(page=page, per_page=per_page, error_out=False)

    return jsonify({
        "total": movies.total,
        "page": movies.page,
        "per_page": movies.per_page,
        "movies": [{
            "id": m.id,
            "title": m.title,
            "rating": float(m.rating) if m.rating else None,
            "num_raters": m.num_raters,
            "quote": m.quote,
            "director": m.director,
            "actors": m.actors,
            "release_date": m.release_date,
            "genres": m.genres,
            "link": m.link
        } for m in movies.items]
    })


# API: 按年份获取电影
@app.route('/movies/year/<year>', methods=['GET'])
def get_movies_by_year(year):
    movies = Movie.query.filter_by(release_date=year).all()
    return jsonify([{
        "id": m.id,
        "title": m.title,
        "rating": float(m.rating) if m.rating else None,
        "num_raters": m.num_raters,
        "quote": m.quote,
        "director": m.director,
        "actors": m.actors,
        "release_date": m.release_date,
        "genres": m.genres,
        "link": m.link
    } for m in movies])


# API: 获取出演次数最多的演员
@app.route('/top_actors', methods=['GET'])
def get_top_actors():
    limit = request.args.get('limit', None, type=int)

    results = db.session.query(Movie.actors).all()
    actor_counts = {}

    for result in results:
        if result.actors:
            # 使用逗号分割多个演员
            actors = [actor.strip() for actor in result.actors.split(",")]
            for actor in actors:
                if actor:  # 避免空值
                    actor_counts[actor] = actor_counts.get(actor, 0) + 1

    # 按次数排序
    top_actors = sorted(actor_counts.items(), key=lambda x: x[1], reverse=True)

    # 如果请求中指定了limit参数，则限制返回数量
    if limit:
        top_actors = top_actors[:limit]

    return jsonify([{"actor": actor, "count": count} for actor, count in top_actors])


# API: 获取出现次数最多的导演
@app.route('/top_directors', methods=['GET'])
def get_top_directors():
    limit = request.args.get('limit', None, type=int)

    results = db.session.query(Movie.director).all()
    director_counts = {}

    for result in results:
        if result.director:
            # 导演字段可能包含多个导演（用斜杠分隔）
            directors = [director.strip() for director in result.director.split("/")]
            for director in directors:
                if director:  # 避免空值
                    director_counts[director] = director_counts.get(director, 0) + 1

    # 按出现次数排序
    top_directors = sorted(director_counts.items(), key=lambda x: x[1], reverse=True)

    # 如果请求中指定了limit参数，则限制返回数量
    if limit:
        top_directors = top_directors[:limit]

    return jsonify([{"director": director, "count": count} for director, count in top_directors])


# API: 获取各类型电影数量
@app.route('/genre_counts', methods=['GET'])
def get_genre_counts():
    results = db.session.query(Movie.genres).all()
    genre_counts = {}

    for result in results:
        if result.genres:
            # 类型字段是以逗号分隔的
            genres = [genre.strip() for genre in result.genres.split(",")]
            for genre in genres:
                if genre:  # 避免空值
                    genre_counts[genre] = genre_counts.get(genre, 0) + 1

    # 按电影数量排序
    sorted_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)

    return jsonify([{"genre": genre, "count": count} for genre, count in sorted_genres])


# API: 按评分筛选电影
@app.route('/movies_by_rating', methods=['GET'])
def get_movies_by_rating():
    min_rating = request.args.get('min_rating', 0, type=float)
    max_rating = request.args.get('max_rating', 10, type=float)

    movies = Movie.query.filter(Movie.rating >= min_rating, Movie.rating <= max_rating).all()

    return jsonify([{
        "id": m.id,
        "title": m.title,
        "rating": float(m.rating) if m.rating else None,
        "num_raters": m.num_raters,
        "quote": m.quote,
        "director": m.director,
        "actors": m.actors,
        "release_date": m.release_date,
        "genres": m.genres,
        "link": m.link
    } for m in movies])


# API: 获取电影年代分布
@app.route('/decade_distribution', methods=['GET'])
def get_decade_distribution():
    results = db.session.query(Movie.release_date).filter(Movie.release_date.isnot(None)).all()
    decade_counts = {}

    for result in results:
        if result.release_date:
            try:
                # 尝试将年份转为整数
                year = int(result.release_date)
                decade = (year // 10) * 10  # 获取年代（1990年代，2000年代等）
                decade_counts[decade] = decade_counts.get(decade, 0) + 1
            except ValueError:
                # 如果无法转为整数，则跳过
                continue

    # 按年代排序
    sorted_decades = sorted(decade_counts.items())

    return jsonify([{"decade": f"{decade}s", "count": count} for decade, count in sorted_decades])


# API: 获取评分统计信息
@app.route('/rating_stats', methods=['GET'])
def get_rating_stats():
    # 计算平均评分
    avg_rating = db.session.query(func.avg(Movie.rating)).scalar()

    # 计算评分分布
    rating_distribution = {}
    for i in range(0, 11):
        min_r = i
        max_r = i + 1
        count = Movie.query.filter(Movie.rating >= min_r, Movie.rating < max_r).count()
        rating_distribution[f"{min_r}-{max_r}"] = count

    # 计算最高评分和最低评分电影
    highest_rated = Movie.query.order_by(Movie.rating.desc()).first()
    lowest_rated = Movie.query.filter(Movie.rating > 0).order_by(Movie.rating).first()

    return jsonify({
        "average_rating": float(avg_rating) if avg_rating else 0,
        "rating_distribution": rating_distribution,
        "highest_rated": {
            "title": highest_rated.title,
            "rating": float(highest_rated.rating) if highest_rated and highest_rated.rating else 0
        } if highest_rated else {},
        "lowest_rated": {
            "title": lowest_rated.title,
            "rating": float(lowest_rated.rating) if lowest_rated and lowest_rated.rating else 0
        } if lowest_rated else {}
    })


# 运行 Flask
if __name__ == '__main__':
    app.run(debug=True)