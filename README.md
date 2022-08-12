# unistore-technicaldemo
A technical demo on [Unistore](https://www.snowflake.com/workloads/unistore/) with an Python UI infront for better comparison. 

Below is the comparison image of the UI outcome. (Please do not forget to watch the [video](https://vimeo.com/739044002/eaa6350080 ) to see the outcome which is less than 3 mins)



![Alt text](static/png/Hybrid_DemoUI.png?raw=true "Sample UI")




The purpose of the demo is to show that Unistore really works!!! Also comparing side by side against standard snowflake tables.

Things performed in this demo are as below.
        1. How adding Primary key and Indexing helps in quick retrieval of the data for Hybrid table against Standard Table
        2. How sington inserts behaves with Hybrid table against Standard Table
        3. How an analytical query works with Hybrid table against Standard Table


The output in the UI show the time taken for each of the steps above, this can be expanded further adding Updates and Deletes or making this [Flask](https://flask.palletsprojects.com/en/2.2.x/) based UI to [Streamlit](https://streamlit.io/) based. 

# prerequisites:
Just few prerequisites.

    1. Knowledge on Snowflake (obviously!!)
    2. Knowledge on Unistore (core concepts of how it works and how it can handle both Transactional and Analytical queries)
    3. Dependent packages 
        1. flask 
        2. flask_debugtoolbar
        3. flask_wtf
        4. wtforms
        5. snowflake.connector
        6. simplejson
        7. pandas
# Steps to be followed
Below is the step by step process for executing this demo.

    1. Download this codebase
    2. pip install the dependent packages
    3. Configure the Snowflake connection
    4. Run the snowflake scripts to create tables for this testing
    5. Run the flask application in your downloads folder (~/Downloads/Projects/Snowflake Accelerator/Snowflake Accelerator.py)
    6. The app will be hosted on http://localhost:5000

# Outcomes
Below are the outcomes that we can find for now.

    1. SELECTS are faster with Hybrid tables when the columns of interest are indexed compared to the Standard Snowflake tables (30 % on an avg faster)
    2. INSERTS are much faster with Hybrid tables compared to Standard Tables (It took only half the time on avg to do singleton inserts for 100 records)
    3. Analytical queries are faster in Standard tables compared to Hybrid tables (10 times faster on avg) BUT THIS WILL IMPROVE as Unistore is still under development

# Demo video
A quick 5 min video of the demo and the findings
https://vimeo.com/739044002/eaa6350080 

