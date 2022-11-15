import streamlit as st
import pandas as pd
from pyspark import SparkContext

import numpy as np
import pandas as pd
from pyspark import SparkConf
from pyspark import SparkContext
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
from operator import add
from pyspark.ml.feature import RegexTokenizer, CountVectorizer
from pyspark.ml.feature import StopWordsRemover, VectorAssembler
from pyspark.ml.feature import Word2Vec, Word2VecModel
from pyspark.ml.feature import IDF
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline, PipelineModel

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from pyspark.sql.functions import *
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import *

st.set_page_config(layout="wide")

def card(df):
    return f"""
        <div class ="d-flex justify-content-center">
    <div class="row" style="width: 1000px;">
    <div class="col">
        <div class="card" style="height: 300px; width:300px; background-color: grey; color:black;">
        <div class="card-body">
            <h5 class="card-title">{df["Course Name"][0]}</h5>
            <p class="card-text">{df["University"][0]}</p>
            <p class="card-text">Difficulty level: {df["Difficulty Level"][0]}</p>
            <div class="card-text">
                <div class="d-flex justify-content-between align-items-center">
                    <div class="ratings">
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star"></i>
                        {df["Course Rating"][0]}
                    </div>
                    <div class="review-count">Reviews</div>
                </div>
            </div>
            <a href="{df["Course URL"][0]}" class="btn btn-dark">Course Link</a>
        </div>
        </div>
    </div>
    
    <div class="col">
        <div class="card" style="height: 300px; width:300px; background-color: grey; color:black;">
        <div class="card-body">
            <h5 class="card-title">{df["Course Name"][1]}</h5>
            <p class="card-text">{df["University"][1]}</p>
            <p class="card-text">Difficulty level: {df["Difficulty Level"][1]}</p>
            <div class="card-text">
                <div class="d-flex justify-content-between align-items-center">
                    <div class="ratings">
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star"></i>
                        {df["Course Rating"][1]}
                    </div>
                    <div class="review-count">Reviews</div>
                </div>
            </div>
            <a href="{df["Course URL"][1]}" class="btn btn-dark">Course Link</a>
        </div>
        </div>
    </div>

    <div class="col">
        <div class="card" style="height: 300px; width:300px; background-color: grey; color:black;">
        <div class="card-body">
            <h5 class="card-title">{df["Course Name"][2]}</h5>
            <p class="card-text">{df["University"][2]}</p>
            <p class="card-text">Difficulty level: {df["Difficulty Level"][2]}</p>
            <div class="card-text">
                <div class="d-flex justify-content-between align-items-center">
                    <div class="ratings">
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star"></i>
                        {df["Course Rating"][2]}
                    </div>
                    <div class="review-count">Reviews</div>
                </div>
            </div>
            <a href="{df["Course URL"][2]}" class="btn btn-dark">Course Link</a>
        </div>
        </div>
    </div>

    </div>
    
    </div>

    <br/>

    <div class ="d-flex justify-content-center">
    <div class="row" style="width: 1000px;">
    <div class="col">
        <div class="card" style="height: 300px; width:300px; background-color: grey; color:black;">
        <div class="card-body">
            <h5 class="card-title">{df["Course Name"][3]}</h5>
            <p class="card-text">{df["University"][3]}</p>
            <p class="card-text">Difficulty level: {df["Difficulty Level"][3]}</p>
            <div class="card-text">
                <div class="d-flex justify-content-between align-items-center">
                    <div class="ratings">
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star"></i>
                        {df["Course Rating"][3]}
                    </div>
                    <div class="review-count">Reviews</div>
                </div>
            </div>
            <a href="{df["Course URL"][3]}" class="btn btn-dark">Course Link</a>
        </div>
        </div>
    </div>
    
    <div class="col">
        <div class="card" style="height: 300px; width:300px; background-color: grey; color:black;">
        <div class="card-body">
            <h5 class="card-title">{df["Course Name"][4]}</h5>
            <p class="card-text">{df["University"][4]}</p>
            <p class="card-text">Difficulty level: {df["Difficulty Level"][4]}</p>
            <div class="card-text">
                <div class="d-flex justify-content-between align-items-center">
                    <div class="ratings">
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star"></i>
                        {df["Course Rating"][4]}
                    </div>
                    <div class="review-count">Reviews</div>
                </div>
            </div>
            <a href="{df["Course URL"][4]}" class="btn btn-dark">Course Link</a>
        </div>
        </div>
    </div>

    <div class="col">
        <div class="card" style="height: 300px; width:300px; background-color: grey; color:black;">
        <div class="card-body">
            <h5 class="card-title">{df["Course Name"][5]}</h5>
            <p class="card-text">{df["University"][5]}</p>
            <p class="card-text">Difficulty level: {df["Difficulty Level"][5]}</p>
            <div class="card-text">
                <div class="d-flex justify-content-between align-items-center">
                    <div class="ratings">
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star rating-color"></i>
                        <i class="fa fa-star"></i>
                        {df["Course Rating"][5]}
                    </div>
                    <div class="review-count">Reviews</div>
                </div>
            </div>
            <a href="{df["Course URL"][5]}" class="btn btn-dark">Course Link</a>
        </div>
        </div>
    </div>

    </div>
    
    </div>
    """

st.markdown(f"""
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-Zenh87qX5JnK2Jl0vWa8Ck2rdkQ2Bzep5IDxbcnCeuOxjzrPF/et3URy9Bv1WTRi" crossorigin="anonymous">
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-OERcA2EqjJCMA+/3y+gxIOqMEjwtxJY7qPCqsdltbNJuaOe923+mo//f6V8Qbsw3" crossorigin="anonymous"></script>
<link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css" rel="stylesheet">
""", unsafe_allow_html=True)

st.markdown("""<style>
.ratings{
    # margin-right:10px;
}

.ratings i{
    
    color:#cecece;
    font-size:14px;
}

.rating-color{
    color:#fbc634 !important;
}

.review-count{
    font-weight:400;
    font-size:12px !important;
}
</style>""", unsafe_allow_html=True)

st.markdown("""<div class ="d-flex justify-content-center"><h1>Course Recommendation System</h1></div>""", unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

course_df = pd.read_csv("course_id.csv")
reviewer_df = pd.read_csv("reviewer_id.csv")

with col2:
    st.subheader("Select preferences for prediction")

    # difficulty = st.selectbox("Course Difficulty", ["Beginner", "Intermediate", "Advanced", "Conversant"])
    difficulty = st.multiselect(
    'Course Difficulty',
    ["Beginner", "Intermediate", "Advanced", "Conversant"],
    ["Beginner"])

    rating = st.select_slider("Course Rating",[4, 4.2, 4.5, 4.7])
    # text = st.text_area("Description of the course")
    rec_type = st.radio(
    "Select recommendation type",
    ('By user', 'By course'))
    # number_ratings = st.select_slider("Number of reviews",[">10,000", ">20,000", ">30,000", ">40,000"])

    if rec_type == "By course":
        crs = st.selectbox(
        'Select course',
        course_df["name"], 2)
    else:
        usr_ = st.selectbox(
        'Select user',
        reviewer_df["reviewers"].unique()[:100])

    clicked = st.button("Get recommendations")





from pyspark.sql import SparkSession

if clicked:
    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()

    course_id = spark.read.csv("course_id.csv",header=True, inferSchema=True)
    reviewer_id = spark.read.csv("reviewer_id.csv",header=True, inferSchema=True)
    reviewer_id.createOrReplaceTempView('reviewers')
    all_course_vecs = np.load('all_course_vecs.npy', allow_pickle=True)

    # print(all_course_vecs)

    def CosineSim(vec1, vec2): 
        return np.dot(vec1, vec2) / np.sqrt(np.dot(vec1, vec1)) / np.sqrt(np.dot(vec2, vec2)) 

    def getSimilarCourse(b_ids, sim_limit = 30):
        
        schema = StructType([   
                                StructField("course_id", StringType(), True)
                                ,StructField("score", IntegerType(), True)
                                ,StructField("input_course_id", StringType(), True)
                            ])
        
        similar_course_df = spark.createDataFrame([], schema)
        
        for b_id in b_ids:
            
            input_vec = [(r[1]) for r in all_course_vecs if r[0] == b_id][0]

            similar_course_rdd = sc.parallelize((i[0], float(CosineSim(input_vec, i[1]))) for i in all_course_vecs)

            similar_course_df = spark.createDataFrame(similar_course_rdd) \
                .withColumnRenamed('_1', 'course_id') \
                .withColumnRenamed('_2', 'score') \
                .orderBy("score", ascending = False)
                
            similar_course_df = similar_course_df.filter(col("course_id") != b_id).limit(sim_limit)
            similar_course_df = similar_course_df.withColumn('input_course_id', lit(b_id))
            
        return similar_course_df

    def getCourseDetails(in_course):
        
        a = in_course.alias("a")
        b = course_id.alias("b")
        
        return a.join(b, col("a.course_id") == col("b.course_id"), 'inner') \
                .select([col('a.'+ xx) for xx in a.columns] + [col('b.course_id'), col('b.name')])

    def getContentRecoms(u_id, sim_limit=30):
    
        # select courses previously reviewed (3+) by the user
        query = """
        SELECT distinct course_id FROM reviewers  
        where rating >= 4.0 
        and reviewers = "{}"
        """.format(u_id)

        usr_rev_course = sqlContext.sql(query)
        
        # from these get sample of 5 courses
        # usr_rev_course = usr_rev_course.sample(False, 0.5).limit(1)

        usr_rev_course_det = getCourseDetails(usr_rev_course)
        
        # show the sample details
        print('\nThe course previously taken by {}:'.format(u_id))
        st.markdown(f"""<div class ="d-flex justify-content-center"><h3>Courses previously taken by {u_id}</h3></div>""", unsafe_allow_html=True)
        temp = usr_rev_course_det.select(['name']).toPandas()
        st.dataframe(temp, use_container_width=True)

        course_list = [i.course_id for i in usr_rev_course.collect()]

        # get courses similar to the sample
        sim_course_df = getSimilarCourse(course_list, sim_limit)

        # filter out those have been reviewd before by the user
        s = sim_course_df.alias("s")
        r = usr_rev_course.alias("r")
        j = s.join(r, col("s.course_id") == col("r.course_id"), 'left_outer') \
            .where(col("r.course_id").isNull()) \
            .select([col('s.course_id'),col('s.score')])

        a = j.orderBy("score", ascending = False).limit(sim_limit)

        return getCourseDetails(a)

    with st.spinner('Getting your recommendations....'):
        if rec_type == "By user":
            u_id = usr_

            content_recom_df = getContentRecoms(u_id)

            print("Courses recommended to user based on his previously taken course:")
            sim_df = content_recom_df.select('name','score').toPandas()

            c_df = pd.read_csv("Coursera.csv")
            c_df = c_df[["Course Name", "University", "Difficulty Level", "Course Rating", "Course URL", "Course Description"]]
            final_df = sim_df.merge(c_df, left_on='name', right_on='Course Name')
            final_df = final_df.sort_values(by=['score'], ascending=False)
            final_df["Course Rating"] = final_df["Course Rating"].astype("float")
            final_df = final_df[final_df["Course Rating"] >= rating]
            final_df = final_df[final_df["Difficulty Level"].isin(difficulty)]
            final_df = final_df.drop_duplicates()
            final_df = final_df.reset_index()
            st.markdown("""<div class ="d-flex justify-content-center"><h1>Recommendations</h1></div>""", unsafe_allow_html=True)
            # st.dataframe(final_df.head(5), use_container_width=True)
            st.markdown(card(final_df), unsafe_allow_html=True)
        
        else:
            crs_ = course_df[course_df["name"] == crs]["course_id"].to_numpy()[0]
            cid = [crs_]

            print('\nThe input course detail:')
            course_id.select('course_id','name') \
                .filter(course_id.course_id.isin(cid) == True).show(truncate=False)

            sims = getCourseDetails(getSimilarCourse(cid))

            print('Top 10 similar courses for input course are:"')
            sim_df = sims.select('name', 'score').toPandas()

            c_df = pd.read_csv("Coursera.csv")
            c_df = c_df[["Course Name", "University", "Difficulty Level", "Course Rating", "Course URL", "Course Description"]]
            final_df = sim_df.merge(c_df, left_on='name', right_on='Course Name')
            final_df = final_df.sort_values(by=['score'], ascending=False)
            final_df["Course Rating"] = final_df["Course Rating"].astype("float")
            final_df = final_df[final_df["Course Rating"] >= rating]
            final_df = final_df[final_df["Difficulty Level"].isin(difficulty)]
            final_df = final_df.drop_duplicates()
            final_df = final_df.reset_index()
            st.markdown("""<div class ="d-flex justify-content-center"><h1>Recommendations</h1></div>""", unsafe_allow_html=True)
            # st.dataframe(final_df.head(5), use_container_width=True)
            st.markdown(card(final_df), unsafe_allow_html=True)
