__author__ = 'Snowflake GSI Team'

from flask import Flask, render_template, request, flash
from flask_debugtoolbar import DebugToolbarExtension
from flask_wtf import FlaskForm
from wtforms import SelectField
import snowflake.connector
import simplejson as json
import datetime
import pandas as pd
import configparser
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

"""
Parameters
"""

warehouse_dict = {
    "xs": 1,
    "s": 2,
    "m": 4,
    "l": 8,
    "xl": 16,
    "2xl": 32,
    "3xl": 64,
    "4xl": 128
}
credit_dict = {
    "aws": {
        "us-east-na": {
            "Standard": 2,
            "Enterprise": 3,
            "Business_critical": 4,
            "on_demand_storage": 40,
            "capacitystorage": 23
        },
        "us-east-ohio": {
            "Standard": 2,
            "Enterprise": 3,
            "Business_critical": 4,
            "on_demand_storage": 40,
            "capacitystorage": 23
        },
        "us-west-oregon": {
            "Standard": 2,
            "Enterprise": 3,
            "Business_critical": 4,
            "on_demand_storage": 40,
            "capacitystorage": 23
        },
        "canada": {
            "Standard": 2.25,
            "Enterprise": 3.5,
            "Business_critical": 4.5,
            "on_demand_storage": 46,
            "capacitystorage": 25
        },
        "eu-london": {
            "Standard": 2.6,
            "Enterprise": 3.9,
            "Business_critical": 5.2,
            "on_demand_storage": 42,
            "capacitystorage": 24
        },
        "eu-ireland": {
            "Standard": 2.5,
            "Enterprise": 3.7,
            "Business_critical": 5,
            "on_demand_storage": 40,
            "capacitystorage": 23
        },
        "eu-frank": {
            "Standard": 2.7,
            "Enterprise": 4,
            "Business_critical": 5.4,
            "on_demand_storage": 45,
            "capacitystorage": 24.5
        },
        "APAC-sydney": {
            "Standard": 2.75,
            "Enterprise": 4.05,
            "Business_critical": 5.5,
            "on_demand_storage": 46,
            "capacitystorage": 25
        },
        "APAC-singapore": {
            "Standard": 2.5,
            "Enterprise": 3.7,
            "Business_critical": 5,
            "on_demand_storage": 46,
            "capacitystorage": 25
        },
        "APAC-tokyo": {
            "Standard": 2.85,
            "Enterprise": 4.3,
            "Business_critical": 5.7,
            "on_demand_storage": 46,
            "capacitystorage": 25
        },
        "APAC-mumbai": {
            "Standard": 2.2,
            "Enterprise": 3.3,
            "Business_critical": 4.4,
            "on_demand_storage": 46,
            "capacitystorage": 25
        }
    },
    "azure": {
        "east-us-2-virginia": {
            "Standard": 2,
            "Enterprise": 3,
            "Business_critical": 4,
            "on_demand_storage": 40,
            "capacitystorage": 23
        },
        "west-us-2-washington": {
            "Standard": 2,
            "Enterprise": 3,
            "Business_critical": 4,
            "on_demand_storage": 40,
            "capacitystorage": 23
        },
        "central-toronto": {
            "Standard": 2.25,
            "Enterprise": 3.50,
            "Business_critical": 4.50,
            "on_demand_storage": 46,
            "capacitystorage": 25
        },
        "west-europe-netherlands": {
            "Standard": 2.5,
            "Enterprise": 3.7,
            "Business_critical": 5,
            "on_demand_storage": 40,
            "capacitystorage": 23
        },
        "australia-east-sydney": {
            "Standard": 2.75,
            "Enterprise": 4.05,
            "Business_critical": 5.5,
            "on_demand_storage": 46,
            "capacitystorage": 25
        },

        "southeast-asia-singapore": {
            "Standard": 2.5,
            "Enterprise": 3.7,
            "Business_critical": 5,
            "on_demand_storage": 46,
            "capacitystorage": 25
        },
        "switzerland-north-zurich": {
            "Standard": 3.10,
            "Enterprise": 4.65,
            "Business_critical": 6.2,
            "on_demand_storage": 50.5,
            "capacitystorage": 28.80
        }
    },
    "gcp": {
        "us-central-1-iowa": {
            "Standard": 2,
            "Enterprise": 3,
            "Business_critical": 4,
            "on_demand_storage": 35,
            "capacitystorage": 20
        },
        "europe-west2-london": {
            "Standard": 2.7,
            "Enterprise": 4,
            "Business_critical": 5.4,
            "on_demand_storage": 40,
            "capacitystorage": 23
        },
        "europe-west4-netherlands": {
            "Standard": 2.5,
            "Enterprise": 3.70,
            "Business_critical": 5,
            "on_demand_storage": 35,
            "capacitystorage": 20
        }
    }
}

app = Flask(__name__)
app.debug = True
app.config['SECRET_KEY'] = '211660'
toolbar = DebugToolbarExtension(app)


def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()


def snow_flake_exec_lambda_handler(event, context):
    print(json.dumps(event))
    # print(get_secret())
    ctx = snowflake.connector.connect(
        user=config['SNOWFLAKE']['User'],
        password=config['SNOWFLAKE']['Password'],
        account=config['SNOWFLAKE']['Account'])
    cs = ctx.cursor()
    one_row = None
    try:
        cs.execute(event['queryStringParameters']['query'])
        one_row = list(cs.fetchone())[0]
    except Exception as e:
        print(e)
    finally:
        cs.close()
        ctx.close()
        return {
            "statusCode": 200,
            "body": json.dumps(str(one_row), default=default)
        }


def get_list_from_snowflake(year):
    ctx = snowflake.connector.connect(
        user=config['SNOWFLAKE']['User'],
        password=config['SNOWFLAKE']['Password'],
        account=config['SNOWFLAKE']['Account'])
    cur = ctx.cursor()
    monthlist = []
    revenuelist = []
    try:
        cur.execute("SELECT month(O_ORDERDATE) as month, round(sum(O_TOTALPRICE)/1000000) as "
                    "Revenue_per_year_in_millions from UNISTORE_163.HYBRID.ORDERS "
                    "group by month(O_ORDERDATE), year(O_ORDERDATE) having year(O_ORDERDATE) = %(year)s order by "
                    "month;" % {
                        "year": year})
        for (month, Revenue_per_year_in_millions) in cur:
            monthlist.append(month)
            revenuelist.append(Revenue_per_year_in_millions)
            print('{0}, {1}'.format(month, Revenue_per_year_in_millions))
    finally:
        cur.close()
        return json.dumps({"month": monthlist, "revenue": revenuelist})


def get_list_from_snowflake_std(year):
    ctx = snowflake.connector.connect(
        user=config['SNOWFLAKE']['User'],
        password=config['SNOWFLAKE']['Password'],
        account=config['SNOWFLAKE']['Account'])
    cur = ctx.cursor()
    monthlist = []
    revenuelist = []
    try:
        cur.execute("SELECT month(O_ORDERDATE) as month, round(sum(O_TOTALPRICE)/1000000) as "
                    "Revenue_per_year_in_millions from UNISTORE_163.HYBRID.STD_ORDERS "
                    "group by month(O_ORDERDATE), year(O_ORDERDATE) having year(O_ORDERDATE) = %(year)s order by "
                    "month;" % {
                        "year": year})
        for (month, Revenue_per_year_in_millions) in cur:
            monthlist.append(month)
            revenuelist.append(Revenue_per_year_in_millions)
            print('{0}, {1}'.format(month, Revenue_per_year_in_millions))
    finally:
        cur.close()
        return json.dumps({"month": monthlist, "revenue": revenuelist})


def filter_nulls(list_input):
    null_list = [' ', '', None]
    if list_input in null_list:
        return True
    else:
        return False


def call_hybrid_insert(InsertOrders):
    query = '{"queryStringParameters": {"query": "call unistore_163.hybrid.insert_into_orders(%(numberofrec)s)"}}' % {
                "numberofrec": InsertOrders}
    print(snow_flake_exec_lambda_handler(json.loads(query), "2"))


def call_std_insert(InsertSTDOrders):
    query = '{"queryStringParameters": {"query": "call unistore_163.' \
            'hybrid.insert_into_std_orders(%(numberofrec)s)"}}' % {
                "numberofrec": InsertSTDOrders}
    print(snow_flake_exec_lambda_handler(json.loads(query), "2"))


class Form(FlaskForm):
    role = SelectField('role', choices=[])
    plot_url = SelectField('plot_url')


# Prod Config #
@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/DataPoints', methods=['GET', 'POST'])
def DataPoints():
    if request.method == 'POST':
        """
        CREATE Database
        """
        try:
            DBs = request.form.getlist('DB[]')
            print(DBs)
            app.logger.warning(DBs)
            if len(DBs) > 0:
                for items in DBs:
                    if len(items) > 0:
                        # query_string = 'CREATE DATABASE IF NOT EXISTS %(db)s' % {"db": items}
                        query = '{"queryStringParameters": {"query": "CREATE DATABASE IF NOT EXISTS %(db)s"}}' % {
                            "db": items}
                        app.logger.warning(snow_flake_exec_lambda_handler(json.loads(query), "2"))
                        flash('CREATE DATABASE is successful')
        except Exception as e:
            flash('Database(s) creation failed :' + str(e))
        """
        CREATE Schemas
        """
        try:
            schemas = request.form.getlist('schema[]')
            app.logger.warning(schemas)
            if len(schemas) > 0:
                for items in schemas:
                    if len(items) > 0:
                        print(str(len(items)))
                        # query_string = 'CREATE DATABASE IF NOT EXISTS %(db)s' % {"db": items}
                        query = '{"queryStringParameters": {"query": "CREATE SCHEMA IF NOT EXISTS %(sch)s"}}' % {
                            "sch": items}
                        app.logger.warning(snow_flake_exec_lambda_handler(json.loads(query), "2"))
                        flash('CREATE SCHEMA is successful')
        except Exception as e:
            flash('Schema(s) creation failed  :' + str(e))
        """
                CREATE Roles
        """
        try:
            roles = request.form.getlist('role[]')
            app.logger.warning(roles)
            if len(roles) > 0:
                for items in roles:
                    if len(items) > 0:
                        print(str(len(items)))
                        # query_string = 'CREATE DATABASE IF NOT EXISTS %(db)s' % {"db": items}
                        query = '{"queryStringParameters": {"query": "CREATE ROLE IF NOT EXISTS %(sch)s"}}' % {
                            "sch": items}
                        app.logger.warning(snow_flake_exec_lambda_handler(json.loads(query), "2"))
                        flash('CREATE ROLE is successful')
        except Exception as e:
            flash('Role(s) creation failed  :' + str(e))
        """
        CREATE Tables
        """
        try:
            tables = request.form.getlist('table[]')
            print(tables)
            app.logger.warning(tables)
            if len(tables) > 0:
                for items in tables:
                    if len(items) > 0:
                        # query_string = 'CREATE DATABASE IF NOT EXISTS %(db)s' % {"db": items}
                        query = '{"queryStringParameters": {"query": "CREATE TABLE IF NOT EXISTS %(tbl)s"}}' % {
                            "tbl": items}
                        app.logger.warning(snow_flake_exec_lambda_handler(json.loads(query), "2"))
                        flash('CREATE TABLE is successful')
        except Exception as e:
            flash('Table(s) creation failed  :' + str(e))
        """
        GRANTS on Accounts
        """
        try:
            accrole = request.form.getlist('accrole[]')
            accprivilege = request.form.getlist('accprivilege[]')
            app.logger.warning(accrole)
            app.logger.warning(accprivilege)
            if len(accrole) > 0 and len(accprivilege) > 0:
                for count, items in enumerate(accprivilege):
                    print(len(items))
                    if len(items) > 0:
                        query = '{"queryStringParameters": {"query": "GRANT %(prv)s ON ACCOUNT TO ROLE %(rle)s"}}' % {
                            "prv": items, "rle": accrole[count]}
                        app.logger.warning(snow_flake_exec_lambda_handler(json.loads(query), "2"))
                        flash('GRANTS on ACCOUNT is successful')
        except Exception as e:

            flash('GRANTS on Accounts failed  :' + str(e))
        """
           GRANTS on Databases
        """
        try:
            dbrole = request.form.getlist('dbrole[]')
            dbprivilege = request.form.getlist('dbprivilege[]')
            db = request.form.getlist('db[]')
            app.logger.warning(dbrole)
            app.logger.warning(dbprivilege)
            if len(dbrole) > 0 and len(dbprivilege) > 0:
                for count, items in enumerate(dbprivilege):
                    if len(items) > 0:
                        query = '{"queryStringParameters": {"query": "GRANT %(prv)s ON DATABASE %(db)s TO ' \
                                'ROLE %(rle)s"}}' \
                                % {"prv": items, "rle": dbrole[count], "db": db[count]}
                        app.logger.warning(snow_flake_exec_lambda_handler(json.loads(query), "2"))
                        flash('GRANTS on Database is successful')
        except Exception as e:
            flash('GRANTS on Databases failed  :' + str(e))
        """
           GRANTS on Schemas
        """
        try:
            schrole = request.form.getlist('schrole[]')
            schprivilege = request.form.getlist('schprivilege[]')
            sch = request.form.getlist('sch[]')
            app.logger.warning(schrole)
            app.logger.warning(schprivilege)
            if len(schrole) > 0 and len(schprivilege) > 0:
                for count, items in enumerate(schprivilege):
                    if len(items) > 0:
                        query = '{"queryStringParameters": {"query": "GRANT %(prv)s ON SCHEMA %(sch)s TO ' \
                                'ROLE %(rle)s"}}' % {"prv": items, "rle": schrole[count], "sch": sch[count]}
                        app.logger.warning(snow_flake_exec_lambda_handler(json.loads(query), "2"))
                        flash('GRANTS on Schemas is successful')
        except Exception as e:
            flash('GRANTS on Schemas failed  :' + str(e))
    return render_template('DataPoints.html')


def get_data_from_hybrid_table(keyid, custid):
    query = '{"queryStringParameters": {"query": "select O_TOTALPRICE as TOTAL_PRICE from UNISTORE_163.HYBRID.ORDERS ' \
            'WHERE O_ORDERKEY = %(key)s and O_CUSTKEY = %(custid)s"}}' % {
                "key": keyid, "custid": custid}
    return snow_flake_exec_lambda_handler(json.loads(query), "2")['body']


def get_data_from_standard_table(keyid, custid):
    query = '{"queryStringParameters": {"query": "select O_TOTALPRICE as TOTAL_PRICE from  ' \
            'UNISTORE_163.HYBRID.STD_ORDERS WHERE O_ORDERKEY = %(key)s and O_CUSTKEY = %(custid)s"}}' % {
                "key": keyid, "custid": custid}
    # print(snow_flake_exec_lambda_handler(json.loads(query), "2")['body'])
    return snow_flake_exec_lambda_handler(json.loads(query), "2")['body']


@app.route('/add', methods=['GET', 'POST'])
def add():
    if request.method == 'POST':
        region = request.form.get('region_list')
        cloud = region.split("-")[0]
        region = region.split('-', 1)[1]
        size = request.form.get('size')
        storage_type = request.form.get('storage_type')
        version_type = request.form.get('version_type')
        warehouse_size = request.form.get('warehouse_size')
        runtime = request.form.get('runtime')
        rundays = request.form.getlist('mycheckbox')
        cluster_cost = 0
        try:
            warehouse_size = request.form.getlist('warehouse_size[]')
            mins = request.form.getlist('mins[]')
            if len(warehouse_size) > 0 and len(mins) > 0:
                for count, items in enumerate(warehouse_size):
                    print(items)
                    if len(items) > 0:
                        cluster_cost = cluster_cost + (credit_dict.get(cloud).get(region).get(version_type) *
                                                       int(warehouse_dict.get(items)) * (
                                                               len(rundays) * 4 * (int(mins[count]) / 60)))
                        app.logger.warning(cluster_cost)
        except Exception as e:
            flash('GRANTS on Schemas failed  :' + str(e))
        storage_cost = credit_dict.get(cloud).get(region).get(storage_type) * float(size)
        compute_cost = cluster_cost
        app.logger.warning(storage_cost)
        app.logger.warning(compute_cost)
        formatted_storage_cost = "${:,.2f}".format(storage_cost)
        formatted_compute_cost = "${:,.2f}".format(compute_cost)
        formatted_total_cost = "${:,.2f}".format(storage_cost + compute_cost)
        flash('Estimated Storage Cost per month:' + str(formatted_storage_cost))
        flash('Estimated Compute Cost per month:' + str(formatted_compute_cost))
        flash('Estimated Total Cost per month:' + str(formatted_total_cost))
    return render_template('add.html')


@app.route("/HybridTable", methods=['GET', 'POST'])
def HybridTable():
    if request.method == 'POST':
        key = request.form.get('keyid')
        keyidNormal = request.form.get('keyidNormal')
        custid = request.form.get('custid')
        custidNormal = request.form.get('custidNormal')
        year = request.form.get('year_list')
        yearstd = request.form.get('year_liststd')
        InsertOrders = request.form.get('InsertOrders')
        InsertSTDOrders = request.form.get('InsertSTDOrders')
        if key:
            dt1 = datetime.datetime.now()
            flash('SALE PRICE OF THE ORDER IS ' + str(get_data_from_hybrid_table(key, custid)))
            dt2 = datetime.datetime.now()
            tdelta = dt2 - dt1
            flash('TIME TAKEN IN HYBRID TABLE is ' + str(tdelta.total_seconds()))
        elif keyidNormal:
            dt1 = datetime.datetime.now()
            flash('SALE PRICE OF THE ORDER IS ' + str(get_data_from_standard_table(keyidNormal, custidNormal)))
            dt2 = datetime.datetime.now()
            tdelta = dt2 - dt1
            flash('TIME TAKEN STANDARD TABLE is ' + str(tdelta.total_seconds()))
        elif year:
            flash('YEAR ' + str(year))
            dt1 = datetime.datetime.now()
            response = get_list_from_snowflake(year)
            data_dic = {
                'month': json.loads(response)['month'],
                'revenue': json.loads(response)['revenue']}
            columns = ['month', 'revenue']
            index = list(range(0, len(json.loads(response)['month'])))
            df = pd.DataFrame(data_dic, columns=columns, index=index)
            table = df.to_html(index=False)
            dt2 = datetime.datetime.now()
            tdelta = dt2 - dt1
            flash('TIME TAKEN HYBRID TABLE is ' + str(tdelta.total_seconds()))
            return render_template(
                "HybridTable.html",
                table=table)
        elif yearstd:
            flash('YEARSTD ' + str(yearstd))
            dt1 = datetime.datetime.now()
            response = get_list_from_snowflake_std(yearstd)
            data_dic = {
                'month': json.loads(response)['month'],
                'revenue': json.loads(response)['revenue']}
            columns = ['month', 'revenue']
            index = list(range(0, len(json.loads(response)['month'])))

            df = pd.DataFrame(data_dic, columns=columns, index=index)
            tablestd = df.to_html(index=False)
            dt2 = datetime.datetime.now()
            tdelta = dt2 - dt1
            flash('TIME TAKEN STANDARD TABLE is ' + str(tdelta.total_seconds()))
            return render_template(
                "HybridTable.html",
                table=tablestd)
        elif InsertOrders:
            dt1 = datetime.datetime.now()
            call_hybrid_insert(InsertOrders)
            dt2 = datetime.datetime.now()
            tdelta = dt2 - dt1
            flash('TIME TAKEN HYBRID TABLE is ' + str(tdelta.total_seconds()))
        elif InsertSTDOrders:
            dt1 = datetime.datetime.now()
            call_std_insert(InsertSTDOrders)
            dt2 = datetime.datetime.now()
            tdelta = dt2 - dt1
            flash('TIME TAKEN STANDARD TABLE is ' + str(tdelta.total_seconds()))

    return render_template('HybridTable.html')


if __name__ == '__main__':
    app.run(host="localhost", port=5000, debug=True)
